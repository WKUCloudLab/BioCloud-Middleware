const ipc=require('node-ipc');
const fs=('fs');
const loadjson = require('load-json-file');
const { spawn } = require('child_process');
const demodirectory = "/data/demoDir";
const out = fs.openSync('./out.log', 'a');
const err = fs.openSync('./err.log', 'a');

const waitqueue = [];
const readyqueue = [];
const inprocessqueue = [];
const childprocesses = {};
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
                        message: "job receieved"
                        
                    }
                );
                var pushed = waitEnqueue(parseInt(data.id));
                pushed.then(pushToReady(result));
            }
        );
    }
);


ipc.server.start();
console.log('listening');

function waitEnqueue(id){
    return new Promise((resolve, reject) =>{
        originalLength = waitqueue.length;
        newLength = waitqueue.push(id);
        if(newLength <= originalLength){
            reject(err)
        }else{
            resolve(waitqueue[newLength - 1]);
        }
            
        

    });
}
function waitSplice(i){
    return new Promise((resolve, reject) => {
        originalLength = waitqueue.length;
       var newlength =  waitqueue.splice(i);
       if(newLength >= originalLength){
        reject(err)
       }else{
        resolve(newlength);
    }
    })
}

function readyEnqueue(id){
    return new Promise((resolve, reject) =>{
        var originalLength = readyqueue.length;
        var newLength = readyqueue.push(id);
        if(newLength <= originalLength){
            reject(err)
        }else{
            resolve(readyqueue[newLength - 1]);
        }           
    });
}

function inprocessEnqueue(id){
    return new Promise((resolve, reject) =>{
        var originalLength = inprocessqueue.length;
        var newLength = inprocessqueue.push(id);
        if(newLength <= originalLength){
            console.log("adding to inprocess queue failed");
            reject(err)
        }else{
            resolve(inprocessqueue[newLength - 1]);
        }
    });
}

function readyDequeue(){
    return new Promise((resolve, reject) =>{
        var originalLength = readyqueue.length;
        var job = readyqueue.pop()
        var newLength = readyqueue.length
        if(newLength >= originalLength){
            console.log("adding to inprocess queue failed");
            reject(err)
        }else{
            resolve(job);
        }
    });
}


//check if it a pipeline or not
function pushToReady(){
    if(waitqueue.length){
        var mysql = require('mysql');
        var connection = mysql.createConnection({
            host     : '192.168.1.100',
            port: '6603',
            user     : 'jamie',
            password : 'poop',
            database : 'BioCloud',
        });
        
        connection.connect(function(err) {
            if (err) throw err;
            console.log("Connected!");
        });

        for(i in waitqueue){
            //search for existing file on server
            //query database for information 
            var sql = "SELECT path FROM files WHERE job_id="+waitqueue[i];
            connection.query(sql, function (err, result) {
                if(err) {
                    throw err;
                    console.log("Error querying database for file information.");
                }
                console.log("successful query!");
                console.log(result);
                for(file in result){
                    if(result[file].path == null){
                        continue; 
                    }
                    fs.access(result[file].path, (err)=>{
                        if(err){
                            throw err;
                            console.log("error in accessing files");
                            continue
                            
                        }
                        readyEnqueue(waitqueue[i]);
                        waitSplice(i);

                    });
                }
            });   
        }
    }
}


setInterval(pushToReady, 10000);

function pushToInprocess(){
    if(inprocessqueue.length < 5){
        var num = 5 - inprocessqueue.length;
        for(i in num){
            var removeready = readyDequeue();
            removeready.then((result) => {

               submitK8s().then(() => inprocessEnqueue(result));

               var mysql = require('mysql');
               var connection = mysql.createConnection({
                   host     : '192.168.1.100',
                   port: '6603',
                   user     : 'jamie',
                   password : 'poop',
                   database : 'BioCloud',
               });
               
               connection.connect(function(err) {
                   if (err) throw err;
                   console.log("Connected!");
               });

               var sql = "INSERT INTO jobs (status) VALUES (?)";
               connection.query(sql,"INPROCESS",  function (err, result) {
                    if(err){
                        console.log("Unable to update job status to INPROCESS");
                        throw err;
                    }
                    console.log("Successfully updated status of job" + result+ " to INPROCESS");
            })expected
        }
    }
}

async function submitK8s(id){
    //build json template here
    buildJson(id).then((id)=> { 
        var submittedJob = await spawn("kubectl",  ["create","-f","./template.json"], {  detached: true, stdio: [ 'ignore', out, err ] });
        //need to start parsing for when job completes here
        handleJobCompletion(id);

    });   
}
//add the submitted job's stdout to an array to regular be searched over and a setinterval function will look to see when job completes then call finalize function that reports job as complete
// it will also update any next_jobs that are depending on it in the database and store the results in the appropriate location  
async function handleJobCompletion(id){
    while(inprocessqueue.length > 0){
        //check constantly for currently running job
        //once done, call complete job function;
        for(i in inprocessqueue){
            if(!childprocesses[inprocessqueue[i]])
            {
                var search = spawn("kubectl", ["logs", "-l", "name=", inprocessqueue[i]],stdio[ 0, 'pipe', err]);
                childprocesses[inprocessqueue[i]] = search;
            }

        }        
    }
}

async buildJson(id){
    return new Promise((resolve, reject) => {
    var mysql = require('mysql');
    var connection = mysql.createConnection({
        host     : '192.168.1.100',
        port: '6603',
        user     : 'jamie',
        password : 'poop',
        database : 'BioCloud',
    });
    
    connection.connect(function(err) {
        if (err){ 
            throw err;
            reject(err);
        }
        console.log("Connected!");
    });

    var sql = "SELECT jobs.id, jobs.name, jobs.status, jobs.start, jobs.end, jobs.next_job, jobs.script_id, jobs.user_id, jobs.pipeline_id, jobs.commands, users.name AS username FROM jobs INNER JOIN users ON  users.id=jobs.user_id";
    connection.query(sql, function (err, result) {
        if(err) {
            throw err;
            reject(err);
            console.log("error querying database");
        }
        console.log("successful query!");
        console.log(result);
        //need to build bash script
        var template = await loadjson('./template.json');
        for (job in result){
            //this might be a string idk, we need it to be json
            var command = JSON.parse(result[job].command);
            var script = result[job].script_id.toLowerCase();
            var scriptname = result[job].name;
            var jobid = (isNAN(result[job].id) ? parseInt(result[job].id) :  result[job].id);
            var userdirectory = "/"+result[job].username;
            let kube_command = `${demodirectory}/test.sh`;    
            template.then((doc)=>{
                
                console.log(doc);
                doc.metadata.name = scriptname + jobid;
                doc.spec.template.spec.containers[0].name = scriptName;
                doc.spec.template.spec.containers[0].image = 'chanstag/' + scriptName;
                doc.spec.template.spec.containers[0].command = ['bash', '-c', '' + kube_command + ''];
                
            });
            writeToScript(command).then(()=> {
                return; 
            });
            



        }
    });
    resolve(id);

 });
}

async writeToScript(command){
    //build string here
    var arglist = "";
    for (args in command.args){
        arglist+=`${ommand.args[args].option} ${command.args[args].args}`;
        
    }
    const bash = arglist;
    await fs.writeFile(`${demodirectory}/test.sh`, bash, (err) => {
        if(err){
            console.log("err writing to bash file" + err); 
            throw err;
        }
        console.log(`wrote successfully to bash script in ${demodirectory}`);
    });
    //update jobs table here to change status to in progress 
}
