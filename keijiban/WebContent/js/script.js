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

function loadRooms(){
    Keijiban.getRoomList(function(reply){
        $list = $(".room-list");

        $list.empty();
        for(var i=0; i<reply.length; i++){
            var $link = $("<a></a>").attr("href", "#").text(reply[i]);
            $link.on('click', {room:reply[i]}, function(e){
                $("#roomInput").val(e.data.room);
                loadMessages(e.data.room);
            });

            $list.append($("<li></li>").append($link));
        }
    });
}
