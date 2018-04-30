const ipc=require('node-ipc');
const queue = require('queue');
/***************************************\
 *
 * You should start both hello and world
 * then you will see them communicating.
 *
 * *************************************/

ipc.config.id = 'world';
ipc.config.retry= 1500;

ipc.serve(
    function(){
        q = queue();
        ipc.server.on(
            'app.message',
            function(data,socket){
                ipc.server.emit(
                    socket,
                    'message',
                    {
                        id: ipc.config.id,
                        
                    }
                );
                queue.push(data.id);
            }
        );
    }
);





ipc.server.start();
console.log('listening');