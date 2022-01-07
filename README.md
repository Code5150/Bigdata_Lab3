# Лабораторная работа № 3: Потоковая обработка в Apache Flink
***

## Задание:
Выполнить следующие задания из набора заданий репозитория https://github.com/ververica/flink-training-exercises:

* RideCleanisingExercise
* RidesAndFaresExercise
* HourlyTipsExerxise
* ExpiringStateExercise


## Выполнение заданий:

### RideCleanisingExercise

__Задание__: *Офильтровать поездки, которые начались и закончились в Нью-Йорке. Вывести получившийся результат.*


__Решение__:

```
val filteredRides = rides
      // filter out rides that do not start and end in NYC
      .filter(ride => GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat))
```

__Тест__:

![Тест RideCleanisingExercise](https://github.com/Code5150/Bigdata_Lab3/blob/master/img/task1.png)


### RidesAndFaresExercise

__Задание__: *Обогатить поездки на такси информацией о плате.*


__Решение__:

```
  class EnrichmentFunction extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
    lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))

    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val fare = fareState.value
      if (fare != null) {
        fareState.clear()
        out.collect((ride, fare))
      }
      else {
        rideState.update(ride)
      }
    }

    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = rideState.value
      if (ride != null) {
        rideState.clear()
        out.collect((ride, fare))
      }
      else {
        fareState.update(fare)
      }
    }
  }
```

__Тест__:

![Тест RidesAndFaresExercise](https://github.com/Code5150/Bigdata_Lab3/blob/master/img/task2.png)


### HourlyTipsExerxise

__Задание__: *Вычислить общее количество чаевых по каждому водителю каждый час, затем из полученного потока данных найти наибольшее колиество чаевых в каждом часе.*


__Решение__:

Основной блок решения:

```
    val tips = fares.map {fare => (fare.driverId, fare.tip)}.keyBy {fare => fare._1}
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .reduce((f1, f2) => (f1._1, f1._2 + f2._2), new WrapWithWindowInfo())

    val maxTipsByHour = tips.windowAll(TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2)
```

__Тест__:

![Тест HourlyTipsExerxise](https://github.com/Code5150/Bigdata_Lab3/blob/master/img/task3.png)



### ExpiringStateExercise

__Задание__: *Обогатить поездки за такси информацией о плате за них.*


__Решение__:

```
 class EnrichmentFunction extends KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
    lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))

    override def processElement1(ride: TaxiRide,
                                 context: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val fare = fareState.value
      if (fare != null) {
        fareState.clear()
        context.timerService.deleteEventTimeTimer(ride.getEventTime)
        out.collect((ride, fare))
      } else {
        rideState.update(ride)
        context.timerService.registerEventTimeTimer(ride.getEventTime)
      }
    }

    override def processElement2(fare: TaxiFare,
                                 context: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = rideState.value
      if (ride != null) {
        rideState.clear()
        context.timerService.deleteEventTimeTimer(ride.getEventTime)
        out.collect((ride, fare))
      } else {
        fareState.update(fare)
        context.timerService.registerEventTimeTimer(fare.getEventTime)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#OnTimerContext,
                         out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (fareState.value != null) {
        ctx.output(unmatchedFares, fareState.value)
        fareState.clear()
      }
      if (rideState.value != null) {
        ctx.output(unmatchedRides, rideState.value)
        rideState.clear()
      }
    }
  }
```

__Тест__:

![Тест ExpiringStateExercise](https://github.com/Code5150/Bigdata_Lab3/blob/master/img/task4.png)

## Заключение

В ходе выполнения лабораторной работы были изучены возможности _Apache Flink_ и успешно выполнены тестовые задания.