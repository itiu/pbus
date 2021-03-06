module Worker;

private import libczmq_header;
private import zmq_point_to_poin_client;
private import zmq_pp_broker_client;
private import Logger;

const PPP_HEARTBEAT_LIVENESS = 5; //  	3-5 is reasonable
const PPP_HEARTBEAT_INTERVAL = 1000 * 10; //  	msecs

int count_expired = 0;

struct Worker
{
	zframe_t* address; //  Address of worker
	string identity; //  Printable identity
	ulong expiry; //  Expires at this time
	zframe_t* client_data; // данные сообщения для обработки
	zframe_t* client_address; // адрес клиента
	bool isBisy = false;
	long time_c_w = 0;
	long time_w_c = 0;

	// поведение:
	//     all (A) - выполняет все операции
	//  writer (W) - только операции записи
	//  reader (R) - только операции чтения
	//  logger (L) - ничего не выполняет а только логгирует операции, параметры logging не учитываются 		
	char behavior = 'A';
};

//  Construct new worker
static Worker* s_worker_new(zframe_t* address, string identity)
{
	Worker* self = new Worker;
	self.address = address;
	self.identity = identity;
	self.expiry = zclock_time() + PPP_HEARTBEAT_INTERVAL * PPP_HEARTBEAT_LIVENESS * 10_000;
	self.client_data = null;
	self.client_address = null;
	return self;
}

//  Destroy specified worker object, including identity frame.
static void s_worker_destroy(Worker** self_p)
{
	assert(self_p);
	if(*self_p)
	{
		Worker* self = *self_p;
		//		writeln ("destroy worker ", self.identity);
		zframe_destroy(&self.address);
		self.identity = null;
		self = null;
		*self_p = null;
	}
}
