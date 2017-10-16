/*
 ***************************************************************************************
 *  Copyright (C) 2006 EsperTech, Inc. All rights reserved.                            *
 *  http://www.espertech.com/esper                                                     *
 *  http://www.espertech.com                                                           *
 *  ---------------------------------------------------------------------------------- *
 *  The software in this package is published under the terms of the GPL license       *
 *  a copy of which has been included with this distribution in the license.txt file.  *
 ***************************************************************************************
 */
package view;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.collection.MultiKeyUntyped;
import com.espertech.esper.collection.OneEventCollection;
import com.espertech.esper.collection.ViewUpdatedCollection;
import com.espertech.esper.core.context.util.AgentInstanceViewFactoryChainContext;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import com.espertech.esper.util.CollectionUtil;
import com.espertech.esper.view.*;
import com.espertech.esper.view.ext.*;
import event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * modified RankWindowView
 */
public class TopkWindowView extends ViewSupport implements DerivedValueView, CloneableView {
    private final TopkWindowViewFactory topkWindowViewFactory;
    protected final ExprEvaluator[] sortCriteriaEvaluators;
    private final ExprNode[] sortCriteriaExpressions;
    protected final ExprEvaluator[] uniqueCriteriaEvaluators;
    private final ExprNode[] uniqueCriteriaExpressions;
    private final EventBean[] eventsPerStream = new EventBean[1];
    private final boolean[] isDescendingValues;
    private final int sortWindowSize;
    private final ViewUpdatedCollection viewUpdatedCollection;
    private final IStreamSortRankRandomAccess optionalRankedRandomAccess;
    protected final AgentInstanceViewFactoryChainContext agentInstanceViewFactoryContext;

    private final Comparator<Object> comparator;

    protected TreeMap<Object, Object> sortedEvents;   // key is computed sort-key, value is either List<EventBean> or EventBean
    protected Map<Object, Object> uniqueKeySortKeys;  // key is computed unique-key, value is computed sort-key
    protected Map<Object, Object> uniqueKeyUnsortKeys;
    protected Map<Object, Object> unsortedEvents;
    protected int numberOfEvents;

    public TopkWindowView(TopkWindowViewFactory topkWindowViewFactory,
                          ExprNode[] uniqueCriteriaExpressions,
                          ExprEvaluator[] uniqueCriteriaEvaluators,
                          ExprNode[] sortCriteriaExpressions,
                          ExprEvaluator[] sortCriteriaEvaluators,
                          boolean[] descendingValues,
                          int sortWindowSize,
                          ViewUpdatedCollection viewUpdatedCollection,
                          IStreamSortRankRandomAccess optionalRankedRandomAccess,
                          boolean isSortUsingCollator,
                          AgentInstanceViewFactoryChainContext agentInstanceViewFactoryContext) {
        this.topkWindowViewFactory = topkWindowViewFactory;
        this.uniqueCriteriaExpressions = uniqueCriteriaExpressions;
        this.uniqueCriteriaEvaluators = uniqueCriteriaEvaluators;
        this.sortCriteriaExpressions = sortCriteriaExpressions;
        this.sortCriteriaEvaluators = sortCriteriaEvaluators;
        this.isDescendingValues = descendingValues;
        this.sortWindowSize = sortWindowSize;
        this.viewUpdatedCollection = viewUpdatedCollection;
        this.optionalRankedRandomAccess = optionalRankedRandomAccess;
        this.agentInstanceViewFactoryContext = agentInstanceViewFactoryContext;

        comparator = CollectionUtil.getComparator(sortCriteriaEvaluators, isSortUsingCollator, isDescendingValues);
        sortedEvents = new TreeMap<Object, Object>(comparator);
        uniqueKeySortKeys = new HashMap<Object, Object>();
        uniqueKeyUnsortKeys = new HashMap<Object, Object>();
        unsortedEvents = new HashMap<Object, Object>();
    }

    public View cloneView() {
        return topkWindowViewFactory.makeView(agentInstanceViewFactoryContext);
    }

    public final EventType getEventType() {
        // The schema is the parent view's schema
        return parent.getEventType();
    }

    public final void update(EventBean[] newData, EventBean[] oldData) {
        if (InstrumentationHelper.ENABLED) {
            InstrumentationHelper.get().qViewProcessIRStream(this, topkWindowViewFactory.getViewName(), newData, oldData);
        }

        Set<EventBean> newEvents = new HashSet<>();
        OneEventCollection removedEvents = new OneEventCollection();

        // Remove old data
        if (oldData != null) {
            for (int i = 0; i < oldData.length; i++) {
                Object uniqueKey = getUniqueValues(oldData[i]);
                Object existingSortKey = uniqueKeySortKeys.get(uniqueKey);

                if (existingSortKey == null) {
                    //added unsortedEvents
                    EventBean event = removeFromUnsortedEvents(uniqueKey);
                    if (event != null) {
                        uniqueKeyUnsortKeys.remove(uniqueKey);
                        removedEvents.add(event);
                    }
                    continue;
                }

                EventBean event = removeFromSortedEvents(existingSortKey, uniqueKey);
                if (event != null) {
                    numberOfEvents--;
                    uniqueKeySortKeys.remove(uniqueKey);
                    removedEvents.add(event);
                    internalHandleRemovedKey(existingSortKey, oldData[i]);

                    //added find top element
                    EventBean topElement = internalHandleRemovedEvent();
                    newEvents.add(topElement);
                }
            }
        }

        // Add new data
        if (newData != null) {
            for (int i = 0; i < newData.length; i++) {
                Object uniqueKey = getUniqueValues(newData[i]);
                Object newSortKey = getSortValues(newData[i]);
                Object existingSortKey = uniqueKeySortKeys.get(uniqueKey);
                Object existingUnSortKey = uniqueKeyUnsortKeys.get(uniqueKey);

                if (existingUnSortKey != null) {
                    Object unsortedKey = uniqueKeyUnsortKeys.get(uniqueKey);
                    if (unsortedKey != null) {
                        int compared = comparator.compare(unsortedKey, newSortKey);
                        if (compared != 0) {
                            EventBean removedEvent = removeFromUnsortedEvents(uniqueKey);
                            if (removedEvent != null)
                                removedEvents.add(removedEvent);
                            uniqueKeyUnsortKeys.remove(uniqueKey);
                        }
                    }
                    tryAddLastElement(newData[i], uniqueKey, newSortKey, newEvents, removedEvents);

                } else if (existingSortKey != null) {
                    int compared = comparator.compare(existingSortKey, newSortKey);

                    if (compared < 0) {
                        compared = comparator.compare(sortedEvents.lastKey(), newSortKey);
                        if (compared < 0) {
                            //find element in unsorted window
                            insertSortedEventsFromUnsortedEvents(newData[i], uniqueKey, newSortKey, existingSortKey, newEvents, removedEvents);
                        } else {
                            updateSortedEvents(newData[i], uniqueKey, newSortKey, existingSortKey, newEvents, removedEvents);
                        }
                    } else if (compared > 0) {
                        updateSortedEvents(newData[i], uniqueKey, newSortKey, existingSortKey, newEvents, removedEvents);
                    } else {
                        //in-place
                        EventBean replaced = replaceSortedEvents(existingSortKey, uniqueKey, newData[i]);
                        if (replaced != null) {
                            //??
                            removedEvents.add(replaced);
                            newEvents.add(newData[i]);
                        }
                        internalHandleReplacedKey(newSortKey, newData[i], replaced);
                    }
                } else {
                    // not currently found: its a new entry
                    tryAddLastElement(newData[i], uniqueKey, newSortKey, newEvents, removedEvents);
                }
            }
        }

        // Remove data that sorts to the bottom of the window
        if (numberOfEvents > sortWindowSize) {
            while (numberOfEvents > sortWindowSize) {
                Object lastKey = sortedEvents.lastKey();
                Object existing = sortedEvents.get(lastKey);
                if (existing instanceof List) {
                    List<EventBean> existingList = (List<EventBean>) existing;
                    while (numberOfEvents > sortWindowSize && !existingList.isEmpty()) {
                        EventBean newestEvent = existingList.remove(0);
                        Object uniqueKey = getUniqueValues(newestEvent);
                        uniqueKeySortKeys.remove(uniqueKey);
                        numberOfEvents--;
                        //removedEvents.add(newestEvent);
                        internalHandleRemovedKey(existing, newestEvent);

                        //added
                        insertUnsortedEvents(newestEvent, uniqueKey, lastKey);
                        newEvents.remove(newestEvent);
                    }
                    if (existingList.isEmpty()) {
                        sortedEvents.remove(lastKey);
                    }
                } else {
                    EventBean lastSortedEvent = (EventBean) existing;
                    Object uniqueKey = getUniqueValues(lastSortedEvent);
                    uniqueKeySortKeys.remove(uniqueKey);
                    numberOfEvents--;
                    removedEvents.add(lastSortedEvent);
                    sortedEvents.remove(lastKey);
                    internalHandleRemovedKey(lastKey, lastSortedEvent);

                    //added
                    insertUnsortedEvents(lastSortedEvent, uniqueKey, lastKey);
                    newEvents.remove(lastSortedEvent);
                }
            }
        }

        // If there are child views, fireStatementStopped update method
        if (optionalRankedRandomAccess != null) {
            optionalRankedRandomAccess.refresh(sortedEvents, numberOfEvents, sortWindowSize);
        }

        EventBean[] newlyArr = null;
        if (!newEvents.isEmpty()) {
            newlyArr = newEvents.toArray(new EventBean[0]);
        }

        EventBean[] expiredArr = null;
        if (!removedEvents.isEmpty()) {
            expiredArr = removedEvents.toArray();
        }

        // update event buffer for access by expressions, if any
        if (viewUpdatedCollection != null) {
            viewUpdatedCollection.update(newData, expiredArr);
        }

        if (this.hasViews()) {
            if (InstrumentationHelper.ENABLED) {
                InstrumentationHelper.get().qViewIndicate(this, topkWindowViewFactory.getViewName(), newlyArr, expiredArr);
            }
            updateChildren(newlyArr, expiredArr);
            if (InstrumentationHelper.ENABLED) {
                InstrumentationHelper.get().aViewIndicate();
            }
        }

        if (InstrumentationHelper.ENABLED) {
            InstrumentationHelper.get().aViewProcessIRStream();
        }
    }

    private void insertSortedEventsFromUnsortedEvents(EventBean newData, Object uniqueKey, Object newSortKey, Object existingSortKey, Set<EventBean> newEvents, OneEventCollection removedEvents) {
        EventBean removed = removeFromSortedEvents(existingSortKey, uniqueKey);
        if (removed != null) {
            numberOfEvents--;
            internalHandleRemovedKey(existingSortKey, removed);

            uniqueKeySortKeys.remove(uniqueKey);
            insertUnsortedEvents(newData, uniqueKey, newSortKey);

            EventBean topEvent = internalHandleRemovedEvent();
            if (removed != topEvent) {
                newEvents.add(topEvent);
                removedEvents.add(removed);
            }
        }
    }

    private void tryAddLastElement(EventBean eventBean, Object uniqueKey, Object newSortKey, Set<EventBean> newEvents, OneEventCollection removedEvents) {
        // determine full or not
        if (numberOfEvents >= sortWindowSize) {
            int compared = comparator.compare(sortedEvents.lastKey(), newSortKey);

            // this new event will fall outside of the ranks or coincides with the last entry, so its an old event already
            if (compared < 0) {
                CollectionUtil.addEventByKeyLazyListMapBack(newSortKey, eventBean, unsortedEvents);
                uniqueKeyUnsortKeys.put(uniqueKey, newSortKey);
            }
            // this new event is higher in sort key then the last entry so we are interested
            else {
                insertSortedEvents(eventBean, uniqueKey, newSortKey, newEvents);
            }
        }
        // not yet filled, need to add
        else {
            insertSortedEvents(eventBean, uniqueKey, newSortKey, newEvents);
        }
    }

    private void updateSortedEvents(EventBean eventBean, Object uniqueKey, Object newSortKey, Object existingSortKey, Set<EventBean> newEvents, OneEventCollection removedEvents) {
        //update element in sorted window
        EventBean removed = removeFromSortedEvents(existingSortKey, uniqueKey);
        if (removed != null) {
            numberOfEvents--;
            removedEvents.add(removed);
            internalHandleRemovedKey(existingSortKey, removed);
            uniqueKeySortKeys.remove(uniqueKey);
        }

        insertSortedEvents(eventBean, uniqueKey, newSortKey, newEvents);
    }

    private void insertSortedEvents(EventBean eventBean, Object uniqueKey, Object newSortKey, Set<EventBean> newEvents) {
        uniqueKeySortKeys.put(uniqueKey, newSortKey);
        numberOfEvents++;
        CollectionUtil.addEventByKeyLazyListMapBack(newSortKey, eventBean, sortedEvents);
        newEvents.add(eventBean);
        internalHandleAddedKey(newSortKey, eventBean);
    }

    private void insertUnsortedEvents(EventBean eventBean, Object uniqueKey, Object newSortKey) {
        uniqueKeyUnsortKeys.put(uniqueKey, newSortKey);
        CollectionUtil.addEventByKeyLazyListMapBack(newSortKey, eventBean, unsortedEvents);
    }

    public void internalHandleReplacedKey(Object newSortKey, EventBean newEvent, EventBean oldEvent) {
        // no action
    }

    public void internalHandleRemovedKey(Object sortKey, EventBean eventBean) {
        // no action
    }

    public void internalHandleAddedKey(Object sortKey, EventBean eventBean) {
        // no action
    }

    private EventBean internalHandleRemovedEvent() {
        if (unsortedEvents.size() < 1)
            return null;

        //added
        Object unsortedKey = Collections.min(unsortedEvents.keySet(), comparator);
        EventBean event = null;

        if (unsortedKey != null) {
            Object existing = unsortedEvents.get(unsortedKey);
            if (existing != null) {
                if (existing instanceof List) {
                    List<EventBean> existingList = (List<EventBean>) existing;
                    Iterator<EventBean> it = existingList.iterator();
                    event = it.next();
                    it.remove();

                    if (existingList.isEmpty()) {
                        unsortedEvents.remove(unsortedKey);
                    }
                } else {
                    event = (EventBean) existing;
                    unsortedEvents.remove(unsortedKey);
                }

                Object uniqueKey = getUniqueValues(event);
                numberOfEvents++;
                uniqueKeySortKeys.put(uniqueKey, unsortedKey);
                CollectionUtil.addEventByKeyLazyListMapBack(unsortedKey, event, sortedEvents);
                uniqueKeyUnsortKeys.remove(uniqueKey);
            }
        }

        return event;
    }

    private EventBean removeFromSortedEvents(Object sortKey, Object uniqueKeyToRemove) {
        Object existing = sortedEvents.get(sortKey);

        EventBean removedOldEvent = null;
        if (existing != null) {
            if (existing instanceof List) {
                List<EventBean> existingList = (List<EventBean>) existing;
                Iterator<EventBean> it = existingList.iterator();
                for (; it.hasNext(); ) {
                    EventBean eventForRank = it.next();
                    if (getUniqueValues(eventForRank).equals(uniqueKeyToRemove)) {
                        it.remove();
                        removedOldEvent = eventForRank;
                        break;
                    }
                }

                if (existingList.isEmpty()) {
                    sortedEvents.remove(sortKey);
                }
            } else {
                removedOldEvent = (EventBean) existing;
                sortedEvents.remove(sortKey);
            }
        }
        return removedOldEvent;
    }

    private EventBean removeFromUnsortedEvents(Object uniqueKeyToRemove) {
        Object unsortedKey = uniqueKeyUnsortKeys.get(uniqueKeyToRemove);

        if (unsortedKey == null)
            return null;

        Object existing = unsortedEvents.get(unsortedKey);

        EventBean removedOldEvent = null;
        if (existing != null) {
            if (existing instanceof List) {
                List<EventBean> existingList = (List<EventBean>) existing;
                Iterator<EventBean> it = existingList.iterator();

                for (; it.hasNext(); ) {
                    EventBean eventForRemove = it.next();
                    if (getUniqueValues(eventForRemove).equals(uniqueKeyToRemove)) {
                        it.remove();
                        removedOldEvent = eventForRemove;
                        break;
                    }
                }

                if (existingList.isEmpty()) {
                    unsortedEvents.remove(unsortedKey);
                }
            } else {
                removedOldEvent = (EventBean) existing;
                unsortedEvents.remove(unsortedKey);
            }
        }

        return removedOldEvent;
    }

    private EventBean replaceSortedEvents(Object sortKey, Object uniqueKeyToReplace, EventBean newData) {
        Object existing = sortedEvents.get(sortKey);

        EventBean replaced = null;
        if (existing != null) {
            if (existing instanceof List) {
                List<EventBean> existingList = (List<EventBean>) existing;
                Iterator<EventBean> it = existingList.iterator();
                for (; it.hasNext(); ) {
                    EventBean eventForRank = it.next();
                    if (getUniqueValues(eventForRank).equals(uniqueKeyToReplace)) {
                        it.remove();
                        replaced = eventForRank;
                        break;
                    }
                }
                existingList.add(newData);  // add to back as this is now the newest event
            } else {
                replaced = (EventBean) existing;
                sortedEvents.put(sortKey, newData);
            }
        }
        return replaced;
    }

    public final Iterator<EventBean> iterator() {
        return new RankWindowIterator(sortedEvents);
    }

    public final String toString() {
        return this.getClass().getName() +
                " uniqueFieldName=" + Arrays.toString(uniqueCriteriaExpressions) +
                " sortFieldName=" + Arrays.toString(sortCriteriaExpressions) +
                " isDescending=" + Arrays.toString(isDescendingValues) +
                " sortWindowSize=" + sortWindowSize;
    }

    public Object getUniqueValues(EventBean theEvent) {
        return getCriteriaKey(eventsPerStream, uniqueCriteriaEvaluators, theEvent, agentInstanceViewFactoryContext);
    }

    public Object getSortValues(EventBean theEvent) {
        return getCriteriaKey(eventsPerStream, sortCriteriaEvaluators, theEvent, agentInstanceViewFactoryContext);
    }

    public static Object getCriteriaKey(EventBean[] eventsPerStream, ExprEvaluator[] evaluators, EventBean theEvent, ExprEvaluatorContext evalContext) {
        eventsPerStream[0] = theEvent;
        if (evaluators.length > 1) {
            return getCriteriaMultiKey(eventsPerStream, evaluators, evalContext);
        } else {
            return evaluators[0].evaluate(eventsPerStream, true, evalContext);
        }
    }

    public static MultiKeyUntyped getCriteriaMultiKey(EventBean[] eventsPerStream, ExprEvaluator[] evaluators, ExprEvaluatorContext evalContext) {
        Object[] result = new Object[evaluators.length];
        int count = 0;
        for (ExprEvaluator expr : evaluators) {
            result[count++] = expr.evaluate(eventsPerStream, true, evalContext);
        }
        return new MultiKeyUntyped(result);
    }

    /**
     * True to indicate the sort window is empty, or false if not empty.
     *
     * @return true if empty sort window
     */
    public boolean isEmpty() {
        if (sortedEvents == null) {
            return true;
        }
        return sortedEvents.isEmpty();
    }

    public void visitView(ViewDataVisitor viewDataVisitor) {
        viewDataVisitor.visitPrimary(sortedEvents, false, topkWindowViewFactory.getViewName(), numberOfEvents, sortedEvents.size());
    }

    public ViewFactory getViewFactory() {
        return topkWindowViewFactory;
    }

    private static final Logger log = LoggerFactory.getLogger(TopkWindowView.class);
}
