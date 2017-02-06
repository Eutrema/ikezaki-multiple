$(document).ready(function(){
    reloadList();
    //ここから下を追加
    $(".nyuukabtn").on('click', function(){
        var input = $("#nyuuka").serializeJson();

        Zaiko.receiveItem(input, function(reply){
            $(".nyuukamsg").text(input.name + "を" + input.amount + "個入荷しました");
                reloadList();
        });
    });
})
function reloadList(){
    Zaiko.getItemList(function(reply){
        var $list = $("#list");

        //表をクリア
        $list.empty();
        $list.append("<tr><th>商品名</th><th>個数</th><th>出荷</th></tr>");

        for(var i=0; i<reply.length; i++){
            //表に追加
            $list.append($("<tr></tr>").append(
                $("<td></td>").text(reply[i].name),
                $("<td></td>").text(reply[i].amount)
            ));
        }
    });
}