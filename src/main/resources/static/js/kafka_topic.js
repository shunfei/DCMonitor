$(function(){
    addActive($('#kafka_nav'));

    var pt = $('#period').html();
    var topic = $('#kafka_topic').html().trim();
    var consumer = $('#kafka_consumer').html().trim();

    createTheam();

    $.post('/kafka/detail', {
        'topic' : topic,
        'consumer' : consumer
    },function(data){
        buildKafkaGraps(data,topic,consumer,true,pt);
    });

    $('#go').click(function(){
       var from = $('#from').val();
       var to = $('#to').val();
        $.post('/kafka/detail', {
            'topic' : topic,
            'consumer' : consumer,
            'from' : from,
            'to' : to
        },function(data){
            buildKafkaGraps(data,topic,consumer);
        });
    });

});



function buildKafkaGraps(data,topic,consumer,needFlush,pt){

    Highcharts.setOptions({
                              global: {
                                  useUTC: false,
                                  timezoneOffset :  timeZoneOffsetHour * 60
                              }
                          });

    var ds = new Array([],[],[]);
    data.map(function(p){
           ds[0].push([p['timeStamp']  , p['logSize']]);
           ds[1].push([p['timeStamp']  , p['offset']]);
           ds[2].push([p['timeStamp'] , p['lag']]);
    });

    $('#container').highcharts('StockChart',{
                                    rangeSelector: {
                                        selected: 1
                                    },
                                   chart: {
                                       zoomType: 'x',
                                       backgroundColor: "#2E3338",
                                       plotBackgroundColor:"#3E444C",
                                       events: {
                                           load: function () {
                                               if(needFlush){
                                                   var s0 = this.series[0];
                                                   var s1 = this.series[1];
                                                   var s2 = this.series[2];
                                                   setInterval(function () {
                                                       $.post('/kafka/detail', {
                                                           'topic' : topic,
                                                           'consumer' : consumer
                                                       },function(data){
                                                           var ds = new Array([],[],[]);
                                                           data.map(function(p){
                                                               ds[0].push([p['timeStamp']  , p['logSize']]);
                                                               ds[1].push([p['timeStamp']  , p['offset']]);
                                                               ds[2].push([p['timeStamp'] , p['lag']]);
                                                           });
                                                           s0.setData(ds[0],true,true,true);
                                                           s1.setData(ds[1],true,true,true);
                                                           s2.setData(ds[2],true,true,true);
                                                       });
                                                   }, parseInt(pt));
                                               }
                                               //初始化rate
                                               var tmp = data;
                                               var len = tmp.length;
                                               var logDiffs = tmp[len-1]['logSize'] - tmp[0]['logSize'];
                                               var offDiffs = tmp[len-1]['offset'] - tmp[0]['offset'];

                                               var millSeconds =   tmp[len-1]['timeStamp'] - tmp[0]['timeStamp'];

                                               $('#pro_rate').html( Math.round(logDiffs
                                                                               * 1000.0
                                                                               / millSeconds));
                                               $('#consu_rate').html(Math.round(offDiffs
                                                                                * 1000.0
                                                                                / millSeconds));

                                               $('.timePeriod').html("over " + Math.round(millSeconds/1000)  + " seconds.");
                                           }

                                       }
                                   },
                                   title: {
                                       text:  consumer + '  :  ' + topic ,
                                        style : {
                                            color : '#FFF'
                                        }
                                   },
                                   xAxis :{
                                       events : {
                                           afterSetExtremes: function (event)
                                           {
                                               // log the min and max of the primary, datetime x-axis
                                               var logDiffs = event.target.series[0].dataMax - event.target.series[0].dataMin;
                                               var offDiffs = event.target.series[1].dataMax - event.target.series[1].dataMin;
                                               var millSeconds = event.max - event.min;

                                               $('#pro_rate').html( Math.round(logDiffs
                                                                       * 1000.0
                                                                       / millSeconds));
                                               $('#consu_rate').html(Math.round(offDiffs
                                                                       * 1000.0
                                                                       / millSeconds));

                                               $('.timePeriod').html("over " + Math.round(millSeconds/1000)  + " seconds.");
                                           }
                                       }
                                   },
                                   yAxis: [
                                       {
                                           title: {
                                               text: "logSize",
                                               style: {
                                                   color: '#4572A7'
                                               }
                                           },
                                           labels: {
                                               style: {
                                                   color: '#4572A7'
                                               }
                                           },
                                           opposite: false
                                       },
                                       {
                                           title: {
                                               text: "Lag",
                                               style: {
                                                   color: '#EC4143'
                                               }
                                           },
                                           labels: {
                                               style: {
                                                   color: '#EC4143'
                                               }
                                           },
                                           opposite: true
                                       }
                                   ],
                                   legend: {
                                       enabled: false
                                   },
                                   plotOptions: {
                                       area: {
                                           fillColor: {
                                               linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1},
                                               stops: [
                                                   [0, Highcharts.getOptions().colors[0]],
                                                   [1, Highcharts.Color(Highcharts.getOptions().colors[0]).setOpacity(0).get('rgba')]
                                               ]
                                           },
                                           marker: {
                                               radius: 2
                                           },
                                           lineWidth: 1,
                                           states: {
                                               hover: {
                                                   lineWidth: 1
                                               }
                                           },
                                           threshold: null
                                       }
                                   },

                                   series: [
                                            {
                                                name: 'logSize',
                                                type : "spline",
                                                yAxis: 0,
                                                data: ds[0],
                                                marker : {
                                                    enabled : true,
                                                    radius : 3
                                                },
                                                tooltip: {
                                                    valueDecimals: 2
                                                }
                                            },
                                            {
                                                name: 'offset',
                                                type : "spline",
                                                yAxis: 0,
                                                data: ds[1],
                                                marker : {
                                                    enabled : true,
                                                    radius : 2
                                                },
                                                tooltip: {
                                                    valueDecimals: 2
                                                }
                                            },
                                            {
                                                name: 'lag',
                                                type : "spline",
                                                yAxis: 1,
                                                data: ds[2],
                                                marker : {
                                                    enabled : true,
                                                    radius : 1
                                                },
                                                tooltip: {
                                                    valueDecimals: 2
                                                }
                                            }
                                   ]
                               });
}

function createTheam(){

    Highcharts.theme = {
        colors: ["#2b908f", "#90ee7e", "#f45b5b", "#7798BF", "#aaeeee", "#ff0066", "#eeaaee",
                 "#55BF3B", "#DF5353", "#7798BF", "#aaeeee"],
        chart: {
            backgroundColor: {
                linearGradient: { x1: 0, y1: 0, x2: 1, y2: 1 },
                stops: [
                    [0, '#2a2a2b'],
                    [1, '#3e3e40']
                ]
            },
            //style: {
            //    fontFamily: "'Unica One', sans-serif"
            //},
            plotBorderColor: '#606063'
        },
        title: {
            style: {
                color: '#E0E0E3',
                fontSize: '20px'
            }
        },
        subtitle: {
            style: {
                color: '#E0E0E3'
            }
        },
        xAxis: {
            gridLineColor: '#707073',
            labels: {
                style: {
                    color: '#E0E0E3'
                }
            },
            lineColor: '#707073',
            minorGridLineColor: '#505053',
            tickColor: '#707073',
            title: {
                style: {
                    color: '#A0A0A3'

                }
            }
        },
        yAxis: {
            gridLineColor: '#707073',
            labels: {
                style: {
                    color: '#E0E0E3'
                }
            },
            lineColor: '#707073',
            minorGridLineColor: '#505053',
            tickColor: '#707073',
            tickWidth: 1,
            title: {
                style: {
                    color: '#A0A0A3'
                }
            }
        },
        tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.85)',
            style: {
                color: '#F0F0F0'
            }
        },
        plotOptions: {
            series: {
                dataLabels: {
                    color: '#B0B0B3'
                },
                marker: {
                    lineColor: '#333'
                }
            },
            boxplot: {
                fillColor: '#505053'
            },
            candlestick: {
                lineColor: 'white'
            },
            errorbar: {
                color: 'white'
            }
        },
        legend: {
            itemStyle: {
                color: '#E0E0E3'
            },
            itemHoverStyle: {
                color: '#FFF'
            },
            itemHiddenStyle: {
                color: '#606063'
            }
        },
        credits: {
            style: {
                color: '#666'
            }
        },
        labels: {
            style: {
                color: '#707073'
            }
        },

        drilldown: {
            activeAxisLabelStyle: {
                color: '#F0F0F3'
            },
            activeDataLabelStyle: {
                color: '#F0F0F3'
            }
        },

        navigation: {
            buttonOptions: {
                symbolStroke: '#DDDDDD',
                theme: {
                    fill: '#505053'
                }
            }
        },

        // scroll charts
        rangeSelector: {
            buttonTheme: {
                fill: '#505053',
                stroke: '#000000',
                style: {
                    color: '#CCC'
                },
                states: {
                    hover: {
                        fill: '#707073',
                        stroke: '#000000',
                        style: {
                            color: 'white'
                        }
                    },
                    select: {
                        fill: '#000003',
                        stroke: '#000000',
                        style: {
                            color: 'white'
                        }
                    }
                }
            },
            inputBoxBorderColor: '#505053',
            inputStyle: {
                backgroundColor: '#333',
                color: 'silver'
            },
            labelStyle: {
                color: 'silver'
            }
        },

        navigator: {
            handles: {
                backgroundColor: '#666',
                borderColor: '#AAA'
            },
            outlineColor: '#CCC',
            maskFill: 'rgba(255,255,255,0.1)',
            series: {
                color: '#7798BF',
                lineColor: '#A6C7ED'
            },
            xAxis: {
                gridLineColor: '#505053'
            }
        },

        scrollbar: {
            barBackgroundColor: '#808083',
            barBorderColor: '#808083',
            buttonArrowColor: '#CCC',
            buttonBackgroundColor: '#606063',
            buttonBorderColor: '#606063',
            rifleColor: '#FFF',
            trackBackgroundColor: '#404043',
            trackBorderColor: '#404043'
        },

        // special colors for some of the
        legendBackgroundColor: 'rgba(0, 0, 0, 0.5)',
        background2: '#505053',
        dataLabelsColor: '#B0B0B3',
        textColor: '#C0C0C0',
        contrastTextColor: '#F0F0F3',
        maskColor: 'rgba(255,255,255,0.3)'
    };

// Apply the theme
    Highcharts.setOptions(Highcharts.theme);
}