$(function(){
    addActive($('#druid_nav'));

    $.post("/druid/realtime_nodes", function(data){
        buildTable( data, $('#druid_realtime'));
        $('#druid_realtime').tablesorter();

    });

    $.post("/druid/middle_manager_nodes", function(data){
        buildTable( data, $('#druid_middle'));
        $('#druid_middle').tablesorter();

    });

    $.post("/druid/coordinator_nodes", function(data){
     buildTable( data, $('#druid_coordinator'));
        $('#druid_coordinator').tablesorter();
    });

    $.post("/druid/coordinator_nodes", function(data){
        buildTable( data, $('#druid_overload'));
        $('#druid_overload').tablesorter();
    });

    $.post("/druid/broker_nodes", function(data){
        buildTable( data, $('#druid_broker'));
        $('#druid_broker').tablesorter();
    });

    $.post("/druid/historical_nodes", function(data){
        buildTable( data, $('#druid_history'));
        $('#druid_history').tablesorter();
    });

    $.post("/druid/coordinator_nodes", function(data){
        buildTable( data, $('#druid_overload'));
        $('#druid_overload').tablesorter();
    });

    $.post("/druid/coordinator_nodes", function(data){
        buildTable( data, $('#druid_overload'));
        $('#druid_overload').tablesorter();
    });

});