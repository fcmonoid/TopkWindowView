# Top-k window view 
code for esper's extension view with top-k concept


## dependencies
* java
* Esper

## get started
```
Configuration config = new Configuration();
EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);
epService.getEPAdministrator().getConfiguration().addPlugInView("custom", "topk", "view.TopkWindowViewFactory");
EPStatement stmt = epService.getEPAdministrator().createEPL(EPL Query);
```

## abstract
Top-k 질의 처리를 위한 오픈소스 복합 이벤트 처리 시스템의 확장
 이벤트 스트림에 대한 Top-k 질의는 현재까지 유입된 이벤트 중 주어진 속성값이 큰 상위 k의 이벤트를 내어주는 것이다. 오픈소스 이벤트스트림 처리기인 Esper 엔진이 Top-k 질의를 효율적으로 지원할 수 있도록 Esper의 기존의 뷰를 확장 구현하였다. 결과적으로 기존의 Esper에서 제공되는 질의문만을 통해서  Top-k 질의를 처리하는것에 비해서 TopkWidnowview를 이용하는 것이 월등한 성능을 보이게 된다. . 또한 공개된 Esper의 소스 프로그램을 분석하고 의미적으로 적합한 방법을 탐구하여 Top-k 질의의 의미를 유지하는 확장 방법을 찾아 구현함으로써 직관적인 확장 방법을 사용했을 때 발생되는 문제를 제거하였다.

## Laboratory Site
[PLAS LAB](http://plas.cnu.ac.kr/)

## University Site
[CNU](http://computer.cnu.ac.kr/)
