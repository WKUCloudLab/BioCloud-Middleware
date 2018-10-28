const ipc = require('node-ipc');
const fs = require('fs');
const stdio = require('stdio');
const loadjson = require('load-json-file');
const jsonfile = require('jsonfile');
const { spawn } = require('child_process');
const demodirectory = "/data/demoDir";

const out = fs.open('./out.log', 'a', () => {console.log("opened out.log")});
const err = fs.open('./err.log', 'a', () => {console.log("opened err.log")});

var waitqueue = [];
var readyqueue = [];
var inprocessqueue = [];
var childprocesses = {};
var jobCompletionSubProcess = []
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
        ipc.server.on(
            'app.message',
            function(data,socket){
                ipc.server.emit(
                    socket,
                    'Job.created',
                    {
                        message: "job receieved"
                        
                    }
                );
                console.log(data.id);
                var pushed = waitEnqueue(parseInt(data.id));
                pushed.then((result) => {
                    console.log(result);
                    
                    pushToReady(result);
                });
            }
        );
    }
);


ipc.server.start();
console.log('listening');

function waitEnqueue(id){
    return new Promise((resolve, reject) =>{
        console.log("wait enqueue");
        originalLength = waitqueue.length;
        newLength = waitqueue.push(id);
        if(newLength <= originalLength){
            reject(new Error("Can't add to wait queue"))
        }else{
            resolve(waitqueue[newLength - 1]);

        var mysql = require('mysql');
        var connection = mysql.createConnection({
            host     : '192.168.1.100',
            port     : '6603',
            user     : process.env.DB_USER2,
            password : process.env.DB_PASS2,
            database : 'BioCloud2',
        });
        }

        connection.connect(function(err) {
            if (err) throw err;
            console.log("Connected!");
        });

        
        let sql = "UPDATE Jobs SET status = 'ENQUEUE' WHERE Jobs.id = ?"
        connection.query(sql,[id], function (err, result) {
            if(err){
                throw err
            }

        });
            
        
        resolve(waitqueue[newLength - 1]);
    });
}
function waitSplice(i){
    return new Promise((resolve, reject) => {
        if(waitqueue.length == 0){
            return;
        }
       var originalLength = waitqueue.length;
       var value =  waitqueue.splice(i, 1);
       var newLength = waitqueue.length;
       if(newLength >= originalLength){
        console.log("Value " + value);
        var err = new Error("Can't remove from wait queue")
        reject(err);
       }else{
           console.log("spliced value = " + value);
        resolve(value);
    }
    })
}

function readyEnqueue(id){
    console.log("Adding to ready queue.");
    return new Promise((resolve, reject) =>{
        var originalLength = readyqueue.length;
        var newLength = readyqueue.push(id);
        if(newLength <= originalLength){
            reject(new Error("Can't add to ready queue"));
        }else{
            console.log(readyqueue[newLength - 1]);
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
            reject(new Error("Can't add to inprocess queue"))
        }else{
            resolve(inprocessqueue[newLength - 1]);
        }
    });
}

function readyDequeue(){
    console.log("ready Dequeu");
    return new Promise((resolve, reject) =>{
        var originalLength = readyqueue.length;
        var job = readyqueue.pop();
        var newLength = readyqueue.length;
        if(newLength >= originalLength){
            var err = new Error("Can't remove from ready queue");
            console.log("adding to inprocess queue failed");
            reject(err);
        }else{
            resolve(job);
        }
    });
}

function inprocessDequeue(){
    return new Promise((resolve,reject) => {
        var originalLength = inprocessqueue.length;
        var job = inprocessqueue.pop();
        var newLength = inprocessqueue.length;
        if(newLength >= originalLength){
            console.log("adding to inprocess queue failed");
            reject(new Error("Can't remove from inprocess queue"));
        }else{
            resolve(job);
        }
    });
}


//check if it a pipeline or not
function pushToReady(){
    console.log("pushToReady");
    if(waitqueue.length > 0){
        var mysql = require('mysql');
        var connection = mysql.createConnection({
            host     : '192.168.1.100',
            port: '6603',
            user     : process.env.DB_USER2,
            password : process.env.DB_PASS2,
            database : 'BioCloud2',
        });
        
        connection.connect(function(err) {
            if (err) throw err;
            console.log("Connected!");
        });

        for(i in waitqueue){
            //search for existing file on server
            //query database for information 
            console.log("wait2 " + waitqueue[i]);
            var sql = "SELECT path, name, locked FROM Files WHERE jobId= ? ";
            connection.query(sql,[waitqueue[i]], function (err, result) {
                if(err) {
                    console.log("Error querying database for file information.");
                    throw err; 
                }
                console.log("successful query!");
                console.log(result);
                for(let file in result){
                    if(result[file].path == null){
                        continue; 
                    }
                    //checking if file exists here
                    console.log("accessing file.");
                    if(file.locked == 'LOCKED'){
                        continue;
                    }

                    fs.access(result[file].path+result[file].name,(err)=>{
                        if(err){
                            console.log("error in accessing files");
                            throw err; 
                        }

                    });

                }
                console.log("Waitqueue[i] " + waitqueue[i]);
                readyEnqueue(waitqueue[i]);
                waitSplice(i);
            });   
        }
    }
}


setInterval(pushToReady, 10000);

async function pushToInProcess(){
        console.log("In pushToInProcess");
        if(inprocessqueue.length < 5){
            console.log("inprocess.length " + inprocessqueue.length);
            var num = 5 - inprocessqueue.length;
            for(var i = 0; i < num; i++){
                if(readyqueue.length == 0){
                    return;
                    // reject("ready queue is empty" + readyqueue.length);
                }
                console.log("line 210");
                var removeready = await readyDequeue();
                

                console.log("line 213");
                console.log(removeready);
                submitK8s(removeready);
				inprocessEnqueue(removeready);
			   
                var mysql = require('mysql');
                var connection = mysql.createConnection({
                    host     : '192.168.1.100',
                    port: '6603',
                    user     : process.env.DB_USER2,
                    password : process.env.DB_PASS2,
                    database : 'BioCloud2',
                });
                
                connection.connect(function(err) {
                    if (err) {
                    // reject(err);   
                    throw err;
                    }
                    console.log("Connected!");
                    
                });

                var sql = "UPDATE Jobs SET jobs.status = 'INPROCESS' WHERE Jobs.id = ?";
                connection.query(sql, [removeready],  function (err, result) {
                    if(err){
                        console.log("Unable to update job status to INPROCESS");
                        // reject(err);
                        throw err;
                    }
                    // resolve()
                    console.log("Successfully updated status of job" + result + " to INPROCESS");
                });
            }
      }
}

setInterval(pushToInProcess, 10000);
// setInterval(spawnJobCompletionHandler, 10000);

async function submitK8s(id){
    console.log('What is this dumb ID? Part 1: '+id);

	//build json template here
    id = await buildJson(id) 
    console.log("line 258");
    try{
    var submittedJob = spawn("kubectl",  ["create","-f","./template.json"], { stdio: [ 'ignore', out, err ] });
    childprocesses[id] = submittedJob;
    submittedJob.on('error', (err)=> {
        console.log("failed to spawn process");
    })
    // subProcessHandler();
    submittedJob.on('exit', (code, signal)=>{
        if(signal == null){
            console.log("line 276", id);
            spawnJobCompletionProcess(id);
        }
        else{
            console.log("error from sub process signal");
        }
        
    })


}catch(err){
        console.log(err);
    }
    //need to start parsing for when job completes here  
}
//add the submitted job's stdout to an array to regular be searched over and a setinterval function will look to see when job completes then call finalize function that reports job as complete
// it will also update any next_jobs that are depending on it in the database and store the results in the appropriate location  
async function spawnJobCompletionProcess(id){
        //this function will be called when prior process terminates
        
        if(typeof childprocesses[id] != "undefined"){
            console.log("child process is not undefined");
        }else{
            console.log("Error, child process undefined! job id =", id);
            return;
        }
            var search = spawn("kubectl", ["logs", "job/"+id],stdio[ 0, 'pipe', err]);
            // console.log(search);
            //console.log(JSON.stringify(search));
            addSubProcessHandlers(search, id);
            jobCompletionSubProcess.push(id); 
                        
}

function addSubProcessHandlers(subProc, id){
    //check if there are active subprocess pipes stored in childprocesses
    //need to do a reverse search to match the values with the keys in the associative array childprocesses
    console.log("adding handlers for", id);
        subProc.on("error", (err)=>{
            console.log("subproc error line 317", err)
        })

        subProc.stdout.on('data', (data)=>{
            console.log("data incoming");
            onData( data, id)});

        subProc.stderr.on("err", (err)=>{
            err = err.toString('utf8');
            console.log(err);
            console.log(err.search("ContainerCreating"));
            if(err.search("ContainerCreating") >= 0){
                console.log("calling addSubProcessHandlers again");
                console.log(err.search("ContainerCreating"));
                setTimeout(()=>{
                    let search = spawn("kubectl", ["logs", "job/"+id],stdio[ 0, 'pipe', err]);
                    addSubProcessHandlers(search, id)}, 5000);
            }
            console.log(`subProc err ${err}`);
        });

        subProc.on('exit', (code, signal)=>{
            console.log("code", code);
            console.log("signal", signal)
            if(code == 1){
                setTimeout(()=>{
                    let search = spawn("kubectl", ["logs", "job/"+id],stdio[ 0, 'pipe', err]);
                    addSubProcessHandlers(search, id)}, 10000);
            }
        });
}

var onData = async function(data, id) {
    if(typeof(data) == "undefined"){
        console.log("sub process data is undefined, this needs to be a string.");
        
    }
    data = data.toString("utf8");
    console.log(data);
    let completed = data.search("Successful Execution")
    let contCreating = data.search("ContainerCreating");
    let startedAnalysis = data.search("Started analysis");
    if(contCreating >= 0){
        console.log("container creating, calling again.")
        setTimeout(()=>{
            let search = spawn("kubectl", ["logs", "job/"+id],stdio[ 0, 'pipe', err]);
            addSubProcessHandlers(search, id);
        }, 5000);
        
    }
    if(completed >= 0){
        //update job table here to reflect the successful completion
        var mysql = require('mysql');
        var connection = mysql.createConnection({
            host  : '192.168.1.100',
            port: '6603',
            user     : process.env.DB_USER2,
            password : process.env.DB_PASS2,
            database : 'BioCloud2',
        });
        
        connection.connect(function(err) {
            if (err) throw err;
            console.log("Connected!");
        });

        var sql = "UPDATE Jobs SET status = 'COMPLETE', end = ?  WHERE Jobs.id = ?";
        var date = await getDate();
        connection.query(sql,[date, inprocessqueue[id]],  function (err, result) {
            if(err) throw err

            console.log(`Job ${id} completed.`)
            inprocessDequeue(inprocessqueue[id]);
        });
        
        return;
    }
    if(startedAnalysis >= 0){
        console.log("see that is starting, call again.");
        setTimeout(()=>{
            let search = spawn("kubectl", ["logs", "job/"+id],stdio[ 0, 'pipe', err]);
            addSubProcessHandlers(search, id);
        }, 5000);
    }
}


function getDate(){
    return new Promise((resolve, reject) => { 
        var current = new Date();
    var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    var days = current.getDate();
    var th = "th";
    var hour = current.getHours();
    var AMPM = 'AM';
    var minute = current.getMinutes();
    if(days === 1 || days === 21) {
      th = "st";
    } else if(days === 2 || days === 22) {
      th = "nd";
    } else if(days === 3 || days === 23) {
      th = "rd";
    }
    if(hour > 12) {
      hour -= 12;
      AMPM = 'PM';
    }
    if(minute < 10) {
      minute = "0"+minute;
    }
    const date = months[current.getMonth()]+" "+days+th+" at "+hour+":"+minute+AMPM;
    if(date != undefined && date != null){
        resolve(date);
    }
    reject("Couldn't form date properly");
    });
    
}

async function buildJson(id){
    return new Promise((resolve, reject) => {
        console.log("building json");
    var mysql = require('mysql');
    var connection = mysql.createConnection({
        host     : '192.168.1.100',
        port: '6603',
        user     : process.env.DB_USER2,
        password : process.env.DB_PASS2,
        database : 'BioCloud2',
    });
    
    connection.connect(function(err) {
        if (err){ 
            throw err;
            reject(err);
        }
        console.log("Connected!");
    });

	console.log("What the heck is Id? "+id);

    var sql = "SELECT Jobs.id, Jobs.name, Jobs.status, Jobs.scriptId, Jobs.userId, Jobs.options FROM Jobs WHERE Jobs.id = ?";
    connection.query(sql, [id], async function (err, result) {
        if(err) {
            console.log("error querying database");
            throw err;
            reject(err);
            
        }
        console.log("successfully queried jobs & users!");
        console.log(result);
        if(result.length == 0){
            console.log("job "+id+" does not exist");
            return
        }
        //need to build bash script

        try{
            var doc = await jsonfile.readFile('./template.json');
        } catch(err){
            console.log("unable to read in json file")
        }
            console.log("line 426" + JSON.stringify(result[0]));
            //this might be a string idk, we need it to be json
            var command = result[0].options;
            var script = result[0].scriptId.toLowerCase();
            var scriptName = result[0].name;
            //for some reason name is the id and not jobs.id?
            var jobId = (Number.isNaN(result[0].id) ? parseInt(result[0].id) :  result[0].id);
            var userdirectory = "/"+result[0].userId;
            let kube_command = `${demodirectory}/test.sh`;    
            
            console.log(doc);
            doc.metadata.name = jobId;
            doc.spec.template.metadata.labels = {"name":scriptName+jobId};
            doc.spec.template.spec.containers[0].name = scriptName;
            doc.spec.template.spec.containers[0].image = 'chanstag/' + scriptName;
            doc.spec.template.spec.containers[0].command = ['bash', '-c', '' + kube_command + ''];

                
            jsonfile.writeFile("./instance.json", doc, (err)=>{
                if(err){
                    console.log("error writing to json file", err);
                }
            });
            writeToScript(command).then().catch((result) =>{console.log("Couldn't write bash script");});
            });
    resolve(id); 

 });
}


async function writeToScript(command){
    //build string here
    await new Promise( async (resolve, reject) => {
    var arglist = "#!/bin/bash\n\n";
    // let args = command.split(' ');
    // for (arg of args){
    //     arglist+=`${arg} ${command.args[args].args}`;
        
    // }
    let args = command.split(" "); 
    arglist+=`${args[0]} ${demodirectory}/${args[1]}\n`;
    arglist+=`ls ${demodirectory}\n`;
    arglist+=`echo "Successful Execution"\n`;

    fs.writeFile(`${demodirectory}/tesh.sh`, arglist, (err)=>{
        if(err) throw err;
        console.log(err);
    })

    //we may need to use this in the future if there are multiple operations being performed on the file. check https://nodejs.org/api/fs.html#fs_fs_write_fd_string_position_encoding_callback
	// var bashFile = fs.createWriteStream(`${demodirectory}/tesh.sh`, {mode: 0o777});
	// bashFile.on('error', function(err) {
	// 	console.log(`error opening file at ${demodirectory}/tesh.sh`, err);
	// 	bashFile.end();
	// });
	// bashFile.write(arglist);
	// bashFile.end();	

    /*
    let writeBash = await fs.writeFile(`${demodirectory}/test.sh`, bash, (err) => {
        if(err){
            console.log("err writing to bash file" + err);
            reject("err writing to test.sh"); 
            throw err;
        }
        resolve("wrote to file in demo directory");
        console.log(`wrote successfully to bash script in ${demodirectory}`);
    });
    */
    //update jobs table here to change status to in progress 
});
}
