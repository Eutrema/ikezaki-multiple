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