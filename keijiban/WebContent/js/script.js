function loadMessages(room){
    Keijiban.getMessages(room, function(reply){
        var $board = $(".keijiban");

        $board.empty();
        $board.append("<h3>" + room + "</h3>");
        for(var i=0; i<reply.length; i++){
            $board.append("<h4 class='name' style='display:inline'>"+reply[i].name+"</h4>");
            $board.append(" <h6 class='time' style='display:inline'>"+reply[i].time+"</h6>");
            $board.append("<p class='message'>"+reply[i].message+"</p>");
        }
    });
}
$(document).ready(function(){
    loadMessages("test");
    $(".less").on('click', function(){
        var input = $("#toukou").serializeJson();

        Keijiban.postMessage(input, function(reply){
            loadMessages(input.room);
        });
    });
});