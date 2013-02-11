module ppqueue;

private import myversion;

private import core.runtime;

private import std.stdio;
private import std.container;
private import std.c.string;
private import std.datetime;
private import std.process;
private import std.conv;
version (linux) import std.c.linux.linux;

private import libzmq_header;
private import libczmq_header;

private import util;

private import Logger;
private import Worker;

private import load_info;

Logger log;
Logger io_msg;
byte trace_msg[1000];

static this()
{
	log = new Logger("pbus", "log", "broker");
	io_msg = new Logger("pbus", "io", "broker");
}

static string cmd = "msg:command";
static string[] cmd__modify_db = ["put", "get_ticket", "remove"];

static int PPP_INTERVAL_INIT = 1000; //  	Initial reconnect
static int PPP_INTERVAL_MAX = 32000; // 	After exponential backoff
static int PPP_WORK_INTERVAL_MAX = 10000; // максимально возможный период для обработки сообщения

static int ZMQ_POLL_MSEC = 1;

static string PPP_HEARTBEAT = "H";
static string PPP_READY = "R";

// поведение:
//	all - выполняет все операции
//  writer - только операции записи
//  reader - только операции чтения
//  logger - ничего не выполняет а только логгирует операции, параметры logging не учитываются 		

static string PPP_BEHAVIOR_ALL = "A";
static string PPP_BEHAVIOR_WRITER = "W";
static string PPP_BEHAVIOR_READER = "R";
static string PPP_BEHAVIOR_LOGGER = "L";

static string NOT_AVAILABLE_WORKERS = "NAW";

void* frontend;
void* backend;

//  List of available workers
Worker*[string] available_workers;

//Called upon a signal from Linux
extern (C) void sighandler(int sig) nothrow @system
{
        printf("signal %d caught...\n", sig);
        try
        {
            system ("kill -kill " ~ text (getpid()));
            //Runtime.terminate();
        }
        catch (Exception ex)
        {
        }
}


void main(char[][] args)
{	
	log.trace_log_and_console("\nPBUS %s.%s.%s\nSOURCE: commit=%s date=%s\n", myversion.major, myversion.minor, myversion.patch,
			myversion.hash, myversion.date);

	string frontend_point = "tcp://*:5544";
	string backend_point = "tcp://*:5566";

	zctx_t* ctx = zctx_new();

	log.trace("bind frontend (client) : %s", frontend_point);
	frontend = zsocket_new(ctx, soc_type.ZMQ_ROUTER);
	zsocket_bind(frontend, cast(char*) frontend_point); //  For clients

	log.trace("bind backend (workers) : %s", backend_point);
	backend = zsocket_new(ctx, soc_type.ZMQ_ROUTER);
	zsocket_bind(backend, cast(char*) backend_point); //  For workers

	Main m = new Main();
	//	stat = new Statistic ();

	LoadInfoThread load_info_thread = new LoadInfoThread(&m.getStatistic);
	load_info_thread.start();

	//  Send out heartbeats at regular intervals
	ulong heartbeat_at = zclock_time() + PPP_HEARTBEAT_INTERVAL;

	int count_h = 0;
	int count_r = 0;
	int count_in_pool = 0;

	zframe_t* frame_heartbeat = zframe_new(cast(char*) PPP_HEARTBEAT, 1);

    version (linux)
    {
        // установим обработчик сигналов прерывания процесса
        signal(SIGABRT, &sighandler);
        signal(SIGTERM, &sighandler);
        signal(SIGQUIT, &sighandler);
        signal(SIGINT, &sighandler);
    }
	
	while(1)
	{
		count_in_pool++;
		zmq_pollitem_t items[] = new zmq_pollitem_t[2];

		items[0].socket = backend;
		items[0].fd = 0;
		items[0].events = io_multiplexing.ZMQ_POLLIN;
		items[0].revents = 0;

		items[1].socket = frontend;
		items[1].fd = 0;
		items[1].events = io_multiplexing.ZMQ_POLLIN;
		items[1].revents = 0;

		//  Poll frontend only if we have available workers
		int rc = zmq_poll(cast(zmq_pollitem_t*) items, available_workers.length ? 2 : 1, PPP_HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
		if(rc == -1)
		{
			log.trace("main loop break, zmq_poll == -1");
			//			break; //  Interrupted
		}

		//  Handle worker activity on backend
		if(items[0].revents & io_multiplexing.ZMQ_POLLIN)
		{
			StopWatch sw;
			sw.start();

			//  Use worker address for routing
			zmsg_t* msg = zmsg_recv(backend);
			if(!msg)
			{
				log.trace("main loop break, zmsg_recv == !msg");
				break; //  Interrupted
			}

			//  Any sign of life from worker means it's ready
			zframe_t* address = zmsg_unwrap(msg);
			//			print_data_from_frame ("W -> Q addr:", address);

			Worker* worker = null;
			string id = null;

			if(address !is null)
			{
				// this is READY or HEARTBEAT message from worker
				id = fromStringz(cast(char*) zframe_data(address), zframe_size(address));
			}

			long msg_size = zmsg_size(msg);

			//  Validate control message, or return reply to client
			if(id !is null)
			{
				if(msg_size == 1)
				{
					if(trace_msg[1])
						log.trace("пришло сообщение от воркера о том что он готов или его это ответ на HEARTBEAT");

					if(trace_msg[2])
						log.trace("найдем воркер по идентификатору [%s] в списке доступных свободных воркеров", id);
					Worker** pp = (id in available_workers);

					if(pp !is null)
					{
						if(trace_msg[3])
							log.trace("воркер найден");
						worker = *pp;
						zframe_destroy(&address); //?		
					} else
					{
						if(trace_msg[4])
							log.trace("среди доступных воркер не найден, зарегестрируем как новый");
						//						pp = (id in bisy_workers);
						//						if (pp is null)
						{
							// или создадим новый
							worker = s_worker_new(address, id);
							available_workers[id] = worker;
							m.stat.registred_workers_count++;
						}
					}

					if(worker !is null)
					{
						zframe_t* frame = zmsg_last(msg);

						byte* data = zframe_data(frame);
						if(*data == *(cast(byte*) PPP_READY) || *data == *(cast(byte*) PPP_HEARTBEAT))
						{
							if(*data == *(cast(byte*) PPP_READY))
							{
								worker.behavior = *(data + 1);
								if(!(worker.behavior == 'A' || worker.behavior == 'R' || worker.behavior == 'W' || worker.behavior == 'L'))
									worker.behavior = 'A';

								if(trace_msg[5])
									log.trace("W->Q register worker [%s] R%s", id, worker.behavior);
							}

							//					writeln ("W->Q R ", aaa, " ", worker.identity, " ", worker.expiry, " ", count_r);
							worker.expiry = zclock_time() + PPP_HEARTBEAT_INTERVAL * PPP_HEARTBEAT_LIVENESS;
							count_r++;
						} else
						{
							if(trace_msg[6])
								log.trace("E: invalid message from worker [%s]", worker.identity);
							zmsg_dump(msg);
						}
					}
					zmsg_destroy(&msg);
				} else if(msg_size == 3)
				{
					zframe_destroy(&address);

					// найдем воркер по идентификатору в списке доступных воркеров
					//					Worker** pp = (id in bisy_workers);
					Worker** pp = (id in available_workers);

					if(pp !is null)
					{
						worker = *pp;
						if(worker.isBisy == false)
						{
							if(trace_msg[7])
								log.trace("пришел результат от воркера [%s], но таковой не числится занятым работой", id);
							worker = null;
						}
					} else
					{
						if(trace_msg[8])
							log.trace("пришел результат от воркера [%s], но таковой у нас не зарегистрирован", id);
					}

					if(worker !is null)
					// && worker.msg !is null)
					{
						// это сообщение с результатом работы от воркера, исходное сообщение находится в worker.msg
						zframe_t* result_frame = zmsg_last(msg);
						byte* data = zframe_data(result_frame);
						long data_size = zframe_size(result_frame);

						string client_address = fromStringz(cast(char*) zframe_data(worker.client_address), zframe_size(
								worker.client_address));

						zmsg_t* result_to_client = zmsg_new();
						zmsg_add(result_to_client, worker.client_address);
						zframe_t* head = zframe_new(cast(byte*) "", 0);
						zmsg_add(result_to_client, head);
						result_frame = zframe_new(data, data_size);
						zmsg_add(result_to_client, result_frame);

						zmsg_send(&result_to_client, frontend);

						m.stat.count_prepared_message++;

						//						sw.stop();
						//						worker.time_w_c = sw.peek().usecs;
						//						log.trace("time W->C: %d, C_>W: %d, total: %d", worker.time_c_w, worker.time_w_c,
						//								worker.time_w_c + worker.time_w_c);

						//						if (trace_msg[100]) 						
						//						io_msg.trace_io (false, client_address, data, data_size);

						zframe_destroy(&worker.client_data);
						worker.client_address = null;
						worker.client_data = null;

						//				zframe_send(&worker.client_address, frontend, ZFRAME_REUSE + ZFRAME_MORE);
						//				zframe_send(&worker.client_address, frontend, ZFRAME_REUSE + ZFRAME_MORE);
						//				zframe_send(&worker.client_address, frontend, ZFRAME_REUSE + ZFRAME_MORE);
						//				zframe_send(&result_frame, frontend, ZFRAME_REUSE);

						//						zmsg_send(&msg, frontend);	
						zmsg_destroy(&msg);

						//						available_workers[worker.identity] = worker;
						//						bisy_workers.remove (worker.identity);
						worker.isBisy = false;

						worker.expiry = zclock_time() + PPP_HEARTBEAT_INTERVAL * PPP_HEARTBEAT_LIVENESS;
					}
				}
			}
			//zmsg_destroy (&msg);			
		}
		if(items[1].revents & io_multiplexing.ZMQ_POLLIN)
		{
			StopWatch sw;
			sw.start();

			//  Now get next client request, route to next worker
			zmsg_t* msg = zmsg_recv(frontend);
			if(!msg)
				break; //  Interrupted

			long msg_size = zmsg_size(msg);

			zframe_t* address = zmsg_unwrap(msg);
			//						print_data_from_frame ("C -> Q addr:", address);
			zframe_t* data = zmsg_pop(msg);
			//						print_data_from_frame ("C -> Q data:", data);

			// выбрать свободного воркера и поручить ему задание
			Worker* worker = task_to_worker(address, data, m);
			//			log.trace ("#4");										

			sw.stop();
			if(worker !is null)
			{
				worker.time_c_w = sw.peek().usecs;
			}

			zmsg_destroy(&msg);
		}

		//  Send heartbeats to idle workers if it's time
		ulong now_time = zclock_time();

		/*
		 if (count_in_pool % 100 == 0)
		 {
		 log.trace ("count_in_pool=%d, tt=%d", count_in_pool, now_time - heartbeat_at);
		 if (available_workers.values.length > 0)
		 log.trace ("Q->W H available_workers.length=%d, count_h=%d", available_workers.length, count_h);
		 }
		 */
		//		if (tt > 1_000_000)
		if(now_time >= heartbeat_at)
		{
			if(trace_msg[9])
				log.trace("count_in_pool=%d, tt=%d", count_in_pool, now_time - heartbeat_at);

			//			if (available_workers.values.length > 0)				
			{
				//								log.trace ("W->Q R available_workers.length=%d, bisy_workers=%d, count_r=%d, count_expired=%d ", available_workers.length, bisy_workers.length, count_r, count_expired);
				//				writeln ("***************************************");
				//				writeln ("count_zctx_malloc ", count_zctx_malloc);
				//				writeln ("count_zframe_malloc ", count_zframe_malloc);
				//				writeln ("count_zlist_malloc ", count_zlist_malloc);
				//				writeln ("count_zmsg_malloc ", count_zmsg_malloc);
				//				writeln ("count_node_malloc ", count_node_malloc);
				//writeln ("count_zmsg_recv ", count_zmsg_recv);
				//writeln ("count_zmsg_send ", count_zmsg_send);
				//writeln ("count_zmsg_destroy ", count_zmsg_destroy);

			}

			// "отправим HEARTBEAT незанятым работой воркерам
			foreach(worker; available_workers.values)
			{
				if(worker.isBisy == false)
				{
					zframe_send(&worker.address, backend, ZFRAME_REUSE + ZFRAME_MORE);
					zframe_send(&frame_heartbeat, backend, ZFRAME_REUSE);

					if(trace_msg[10])
						log.trace("send heartbeat to [%s]", worker.identity);

					count_h++;
				}
			}

			heartbeat_at = zclock_time() + PPP_HEARTBEAT_INTERVAL;
		}

		StopWatch sw;
		sw.start();

		//		writeln ("s_workers_purge(available_workers, m)");
		s_workers_purge(available_workers, m);

		sw.stop();
		// tt = sw.peek().usecs;
		//						log.trace ("time s_workers_purge: %d", tt);						
		//		s_workers_purge(bisy_workers);		
	}

	log.trace("main loop is ended");
	writeln("main loop is ended, main pid:", getpid());

	//  When we're done, clean up properly
	foreach(worker; available_workers.values)
	{
		s_worker_destroy(&worker);
	}
	zsocket_destroy(ctx, backend);
	zsocket_destroy(ctx, frontend);

	zctx_destroy(&ctx);

	system("kill -kill " ~ text(getpid()));

	return;
}

bool check_db_update_command(byte* data_b, long data_b_size)
{
	int qq = 0;
	int i;
	for(i = 0; i < data_b_size; i++)
	{
		if(*(data_b + i) == cmd[qq])
		{
			qq++;
			if(qq >= cmd.length)
				break;
		} else
			qq = 0;
	}

	if(qq >= cmd.length)
	{
		// нашли команду msg:command, считаем что именно за комманда
		int s_pos = 0;

		// пропустим символы до [:]
		for(; i < data_b_size; i++)
			if(*(data_b + i) == ':')
			{
				// пропустим символы до ["]
				for(; i < data_b_size; i++)
					if(*(data_b + i) == '"')
					{
						s_pos = i + 1;

						// до следующей [""] будет сама команда
						for(i = s_pos; i < data_b_size; i++)
							if(*(data_b + i) == '"')
							{
								int e_pos = i;

								foreach(cmd; cmd__modify_db)
								{
									if(*(data_b + s_pos) == cmd[0] && e_pos - s_pos == cmd.length)
									{
										// вероятно это нужная нам команда

										int j = 1;
										// сравним остальные символы
										for(i = s_pos + 1; i < e_pos; i++)
										{
											if(cmd[j] != *(data_b + i))
												break;
											j++;
										}

										if(j == cmd.length)
										{
											// найдена команда изменяющая базу данных, выполним ее для всех воркеров

											//											printf("found command:");
											//											for(i = s_pos; i < e_pos; i++)
											//												printf("%c", *(data_b + i));
											//											printf("\n");
											return true;
										}
									}

								}

								break;
							}

						break;
					}

				break;
			}

	}
	return false;
}

Worker* task_to_worker(zframe_t* address, zframe_t* data, Main m)
{

	byte* data_b = zframe_data(data);
	long data_b_size = zframe_size(data);

	bool is_update_command = check_db_update_command(data_b, data_b_size);

	// выбрать свободного воркера
	Worker* worker = null;

	foreach(wrk; available_workers.values)
	{
		// здесь мы выбираем свободного воркера
		// если пришла команда на изменение базы данных то 
		// 		выбираем воркера с поведением WRITER
		// 		если WRITER не нашлось, то берем первого свободного из списка
		// если команда чтения, то отдадим любому воркеру кроме WRITER

		if(wrk.isBisy == false)
		{
			if(is_update_command == true)
			{
				if(wrk.behavior == 'W')
				{
					worker = wrk;
					break;
				}

				if(worker is null)
					worker = wrk;
			} else if(wrk.behavior != 'W')
			{
				worker = wrk;
				break;
			}
		}
	}

	//	StopWatch sw;
	//		    sw.start();

	if(worker !is null)
	{
		// берем первый воркер из списка
		//		worker = available_workers.values[0];

		//	убрать воркер из доступных (для того, что-бы ему не посылались HEARTBEAT)
		//		available_workers.remove (worker.identity);				
		//		bisy_workers[worker.identity] = worker;
		worker.isBisy = true;

		//	отдать ему задачу
		worker.client_address = address;
		worker.client_data = data;
		worker.expiry = zclock_time() + PPP_WORK_INTERVAL_MAX * 10_000;

		zframe_send(&worker.address, backend, ZFRAME_REUSE + ZFRAME_MORE);
		zframe_send(&worker.address, backend, ZFRAME_REUSE + ZFRAME_MORE);
		zframe_send(&worker.address, backend, ZFRAME_REUSE + ZFRAME_MORE);
		zframe_send(&worker.client_data, backend, ZFRAME_REUSE);

		//		byte* data_b = zframe_data(worker.client_data);
		//		int data_b_size = zframe_size(worker.client_data);

		//			        sw.stop();
		//			long tt = sw.peek().usecs;
		//			log.trace ("time C->W (3): %d", tt);						

		// запишем в лог отданное сообщение
		//		io_msg.trace_io (true, worker.identity, data_b, data_b_size);

		return worker;
	} else
	{
		log.trace("нет доступных воркеров, отправим клиенту сообщение о повторении попытки");
		//		printf ("!");    

		//	если свободных нет, ответить клиенту чтоб подождал и попробовал еще раз

		zmsg_t* result_to_client = zmsg_new();
		zmsg_add(result_to_client, address);
		zframe_t* head = zframe_new(cast(byte*) "", 0);
		zmsg_add(result_to_client, head);
		zframe_t* result_frame = zframe_new(cast(char*) NOT_AVAILABLE_WORKERS, NOT_AVAILABLE_WORKERS.length);
		zmsg_add(result_to_client, result_frame);

		zmsg_send(&result_to_client, frontend);

		m.stat.count_waiting_message++;

		//			        sw.stop();
		//			long tt = sw.peek().usecs;
		//			log.trace ("time C->W (3): %d", tt);						
		//		log.trace ("#3");										

		return null;
	}

}

//  Look for & kill expired workers. Workers are oldest to most recent,
//  so we stop at the first alive worker.

static void s_workers_purge(ref Worker*[string] workers, Main m)
{
	ulong now = zclock_time();
	foreach(worker; workers.values)
	{
		//		if(now > worker.expiry)
		if(worker.identity !is null && now > worker.expiry)
		{
			log.trace("worker {%x}[%s] is expired ", worker, worker.identity);

			count_expired++;
			workers.remove(worker.identity);
			m.stat.registred_workers_count--;

			if(worker.client_data !is null)
			{
				// это воркер который завис при выполнении задания
				// передадим его задание другому воркеру
				log.trace("это воркер который завис при выполнении задания %s", worker.identity);

				Worker* a_worker = task_to_worker(worker.client_address, worker.client_data, m);
				//				log.trace ("#3.1");										
				if(a_worker !is null)
				{
					log.trace("перепоручили задание другому %s", a_worker.identity);
				}
				//				log.trace ("#5");										

				worker.client_address = null;
				worker.client_data = null;
			}

			s_worker_destroy(&worker);
		}
	}
}

class Main
{
	Statistic stat;

	this()
	{
		this.stat = new Statistic();
	}

	Statistic getStatistic()
	{
		return stat;
	}

}
