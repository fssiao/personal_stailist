$(document).ready(function(){
    $("#send-btn").click(function(){
        var message = $("#message-input").val();
        $('#message-box').append('<div class="d-flex justify-content-end mb-4"> <div class="bg-success rounded py-2 px-3">'+message+'</div> </div>');
        $('#message-input').val('');
        send_chat(message);
    });

    // Scroll Animation
    $('.section-link').click(function(){
        var id = $(this).text();
        $("html, body").animate({  scrollTop: $("#"+id).offset().top});
    });
    

});

function send_chat(message) {
        $.ajax({
        url: 'http://localhost:5000/chat',
        method: 'GET',
        async: false,
        data: {
            message: message,
        },
        beforeSend: function() {
            $('#message-box').append('<div class="d-flex justify-content-start mb-2 typing-indicator"><div class="bg-light text-dark rounded-pill px-4 py-2"><span class="dot">.</span><span class="dot">.</span><span class="dot">.</span></div></div>');
        },
        success: function (response) {
            $('#message-box').append('<div class="d-flex justify-content-start my-2"> <div class="bg-warning rounded py-2 px-3">'+response+'</div></div>');
        },
        complete: function () {
            $('.typing-indicator').remove();
        },
        error: function () {
            console.log('error');
            $('#message-box').append('<div class="d-flex justify-content-start my-2"> <div class="bg-warning rounded py-2 px-3">Timeout Error</div></div>');
        }
    })
    }