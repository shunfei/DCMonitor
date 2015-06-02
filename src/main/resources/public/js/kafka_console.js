var zkHosts = new Array();
$(document).ready(function() {

    addActive($('#kafka_nav'));
    $.get('/kafka/hosts', function(data) {
        buildTable(data, $('#kafka_table'));
        $('#kafka_table').tablesorter({sortList: [ [0,0]]});
    });

    $.get('/kafka/active',function(data){
        buildTable(data,$('#kafka_active'));
        $('#kafka_active').tablesorter();
        $('#kafka_active').find('tbody tr').each(function(){
            var p = $(this);
            var topic = p.find('td').eq(0).find('a').html().trim();
            var consumer = p.find('td').eq(1).find('a').html().trim();
            p.find('td a').attr("href", "/kafka/topic/"+topic +"?consumer=" + consumer+ "&type=normal").addClass("mtr");
        });
    });

    $.get('/kafka/storm_kafka',function(data){
        buildTable(data,$('#storm_kafka_client'));
        $('#storm_kafka_client').find('tbody tr').each(function(){
            var p = $(this);
            var topic = p.find('td').eq(0).find('a').html().trim();
            var clientId = p.find('td').eq(1).find('a').html().trim();
            p.find('td a').attr("href", "/kafka/topic/"+topic +"?consumer=" + clientId + "&type=storm").addClass("mtr");
        });
    });

    $.get('/kafka/topic',function(data){
        buildTable(data,$('#kafka_topic'));
    });

    $.get('/kafka/consumer',function(data){
        buildTable(data,$('#kafka_consumer'));
    });


});