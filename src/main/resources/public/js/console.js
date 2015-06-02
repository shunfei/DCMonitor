// requires tableHelper

$(function(){

    $('.plus').click(function(){
        var $elem = $(this).parent().next();
        $elem.toggle();
        if($(this).html() == "-"){
            $(this).html("+");
        }else{
            $(this).html("-");
        }
    })
})


function addActive($li){
    $li.siblings().removeClass("active");
    $li.addClass("active");
}

function buildTable(data, jq){
    jq.html("");
    var html = buildHead(data);
    html += buildCells(data);
    jq.html(html);
}

function buildHead(data){
    var html = "<thead><tr>";
    for(var i in data){
        for(var j in data[i]){
            html += "<td>" + j + "</td>";
        }
        break;
    }
    html += "</tr></thead>"
    return html;
}

function buildCells(data){
    var html = "<tbody>";
    for(var i in data){
        html += "<tr>"
        for(var j in data[i]){
            html += "<td><a>" + data[i][j] + "</a></td>";
        }
        html += "</tr>"
    }
    html += "</tbody>"
    return html;
}

function buildRotatTable(data,jq){
    jq.html("");
    var html = "";

    var arr = new Array();
    var brr = new Array();

    for(var i in data){
        for(var j in data[i]){
           brr = brr.concat(j);
        }
        break;
    }

    for(var i in data){
        for(var j in data[i]){
            if(! arr[j]){
                arr[j] = new Array();
            }
            arr[j] = arr[j].concat(data[i][j]);
        }
    }

    for(var i in brr){
        html += "<tr><td>"+ brr[i] + "</td>"
        for(var j in arr[brr[i]]){
            html += "<td>" + arr[brr[i]][j] + "</td>";
        }
        html += "</tr>"
    }
    jq.html(html);
}


