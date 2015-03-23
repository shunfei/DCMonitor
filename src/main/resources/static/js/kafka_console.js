var zkHosts = new Array();
$(document).ready(function() {
    $.get('/kafka/hosts', function(data) {
        buildTable(data, $('#kafka_table'));
    });

    $.get('/kafka/active',function(data){
        buildTable(data,$('#kafka_active'));
        $('#kafka_active').find('tbody tr').each(function(){
            var p = $(this);
            var topic = p.find('td').eq(0).find('a').html().trim();
            var consumer = p.find('td').eq(1).find('a').html().trim();
            p.find('td a').attr("href", "/kafka/topic/"+topic +"?consumer=" + consumer).addClass("mtr");
        });
    });

    $.get('/kafka/topic',function(data){
        buildTable(data,$('#kafka_topic'));
    });

    $.get('/kafka/consumer',function(data){
        buildTable(data,$('#kafka_consumer'));
    });


});