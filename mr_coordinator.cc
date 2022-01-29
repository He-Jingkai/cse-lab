#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

struct Task {
	int taskType;     // should be either Mapper or Reducer
	bool isAssigned;  // has been assigned to a worker
	bool isCompleted; // has been finised by a worker
	int index;        // index to the file
};

class Coordinator {
public:
	Coordinator(const vector<string> &files, int nReduce);
	mr_protocol::status askTask(int, string &reply);
	mr_protocol::status submitTask(int taskType, int index, bool &success);
	
	bool isFinished;

	/* NOT USED */

	// bool isFinishedMap();

    // bool isFinishedReduce();

    // bool Done();
private:
	vector<string> files;
	vector<Task> mapTasks;
	vector<Task> reduceTasks;

	mutex mtx;

	long completedMapCount;
	long completedReduceCount;

	/* NOT USED */

	// string getFile(int index);

	
};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int id, string &reply) {
	// cout<<"id "<<id<<" askTask"<<endl;
	this->mtx.lock();
	if(this->completedMapCount < long(this->mapTasks.size())){
		for(Task& task:mapTasks)
			if(!task.isAssigned){
				task.isAssigned=true;
				reply="1\t"+to_string(task.index)+"\t"+to_string(mapTasks.size())+"\t"+this->files[task.index];
				// cout<<reply<<endl;
				this->mtx.unlock();
				return mr_protocol::OK;
			}
		reply="0\t0\t0\t\\0";
		// cout<<reply<<endl;
		this->mtx.unlock();
		return mr_protocol::OK;
	}
	if(this->completedReduceCount < long(this->reduceTasks.size())){
		for(Task& task:reduceTasks)
			if(!task.isAssigned){
				task.isAssigned=true;
				reply="2\t"+to_string(task.index)+"\t"+to_string(mapTasks.size())+"\t"+"\\0";
				// cout<<reply<<endl;
				this->mtx.unlock();
				return mr_protocol::OK;
			}
		reply="0\t0\t0\t\\0";
		// cout<<reply<<endl;
		this->mtx.unlock();
		return mr_protocol::OK;
	}
	reply="0\t0\t0\t\\0";
	// cout<<reply<<endl;
	this->mtx.unlock();
	return mr_protocol::OK;
}


mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
	// cout<<"id "<<index<<" submit"<<endl;
	this->mtx.lock();
	if(taskType==mr_tasktype::MAP){
		mapTasks[index].isCompleted=true;
		completedMapCount++;
	}
	else if(taskType==mr_tasktype::REDUCE){
		reduceTasks[index].isCompleted=true;
		completedReduceCount++;
	}
	isFinished=(this->completedMapCount >= long(this->mapTasks.size()))&&(this->completedReduceCount >= long(this->reduceTasks.size()));
	success=true;
	this->mtx.unlock();
	// cout<<"id "<<index<<" submit successfully"<<endl;
	return mr_protocol::OK;
}

/* NOT USED */

// string Coordinator::getFile(int index) {
//     this->mtx.lock();
//     string file = this->files[index];
//     this->mtx.unlock();
//     return file;
// }

// bool Coordinator::isFinishedMap() {
//     bool isFinished = false;
//     this->mtx.lock();
//     if (this->completedMapCount >= long(this->mapTasks.size())) {
//         isFinished = true;
//     }
//     this->mtx.unlock();
//     return isFinished;
// }

// bool Coordinator::isFinishedReduce() {
//     bool isFinished = false;
//     this->mtx.lock();
//     if (this->completedReduceCount >= long(this->reduceTasks.size())) {
//         isFinished = true;
//     }
//     this->mtx.unlock();
//     return isFinished;
// }

// //
// // mr_coordinator calls Done() periodically to find out
// // if the entire job has finished.
// //
// bool Coordinator::Done() {
//     bool r = false;
//     this->mtx.lock();
//     r = this->isFinished;
//     this->mtx.unlock();
//     return r;
// }


//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce)
{
	this->files = files;
	this->isFinished = false;
	this->completedMapCount = 0;
	this->completedReduceCount = 0;

	int filesize = files.size();
	for (int i = 0; i < filesize; i++) {
		this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
	}
	for (int i = 0; i < nReduce; i++) {
		this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
	}
}

int main(int argc, char *argv[])
{
	int count = 0;

	if(argc < 3){
		fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
		exit(1);
	}
	char *port_listen = argv[1];
	
	setvbuf(stdout, NULL, _IONBF, 0);

	char *count_env = getenv("RPC_COUNT");
	if(count_env != NULL){
		count = atoi(count_env);
	}

	vector<string> files;
	char **p = &argv[2];
	while (*p) {
		files.push_back(string(*p));
		++p;
	}

	rpcs server(atoi(port_listen), count);

	Coordinator c(files, REDUCER_COUNT);
	
	//
	// Lab2: Your code here.
	// Hints: Register "askTask" and "submitTask" as RPC handlers here
	// 
	server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
 	server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);


	while(!c.isFinished) {
		sleep(2);
	}

	return 0;
}


