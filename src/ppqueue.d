module ppqueue;

private import libzmq_headers;
private import std.stdio;
private import std.c.string;
private import std.datetime;

private import tango.util.uuid.NamespaceGenV5;
private import tango.util.digest.Sha1;
private import tango.util.uuid.RandomGen;
private import tango.math.random.Twister;

private import std.container;
private import Logger;
private import dzmq;
private import Worker;

private import load_info;

Logger log;
Logger io_msg;

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

static int ZMQ_POLL_MSEC = 1000; // http://www.zeromq.org/docs:3-0-upgrade

static string PPP_HEARTBEAT = "H";
static string PPP_READY = "R";
static string NOT_AVAILABLE_WORKERS = "NAW";

void* frontend;
void* backend;

//  List of available workers
Worker*[string] available_workers;

////  List of idle workers
//Worker*[string] bisy_workers;

void main(char[][] args)
{
	log.trace("start");

	string frontend_point = "tcp://*:5544";
	string backend_point = "tcp://*:5566";

	if(zmq_ver.ZMQ_VERSION_MAJOR == 3)
		ZMQ_POLL_MSEC = 1;

	zctx_t* ctx = zctx_new();

	log.trace("bind frontend (client) : %s", frontend_point);
	frontend = zsocket_new(ctx, soc_type.ZMQ_ROUTER);
	zsocket_bind(frontend, frontend_point); //  For clients

	log.trace("bind backend (workers) : %s", backend_point);
	backend = zsocket_new(ctx, soc_type.ZMQ_ROUTER);
	zsocket_bind(backend, backend_point); //  For workers

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
		int rc = zmq_poll(cast(zmq_pollitem_t*) items, available_workers.length ? 2 : 1,
				PPP_HEARTBEAT_INTERVAL * 10 * ZMQ_POLL_MSEC);
		if(rc == -1)
			break; //  Interrupted

		//  Handle worker activity on backend
		if(items[0].revents & io_multiplexing.ZMQ_POLLIN)
		{
			StopWatch sw;
			sw.start();

			//  Use worker address for routing
			zmsg_t* msg = zmsg_recv(backend);
			if(!msg)
				break; //  Interrupted

			//  Any sign of life from worker means it's ready
			zframe_t* address = zmsg_unwrap(msg);
			//			print_data_from_frame ("W -> Q addr:", address);

			Worker* worker = null;
			string id = null;

			if(address !is null)
			{
				// this is READY or HEARTBEAT message from worker
				id = zframe_strdup(address);
			}

			int msg_size = zmsg_size(msg);
			//			printf ("W->Q msg_size=%d\n", msg_size);

			//  Validate control message, or return reply to client
			if(id !is null)
			{
				if(msg_size == 1)
				{
					// найдем воркер по идентификатору в списке доступных // свободных воркеров
					Worker** pp = (id in available_workers);

					if(pp !is null)
					{
						worker = *pp;
						zframe_destroy(&address); //?		
					} else
					{
						// проверим вдруг это пришел HEARTBEAT от воркера который сейчас занят
						//						pp = (id in bisy_workers);

						//						if (pp is null)
						{
							// или создадим новый
							worker = s_worker_new(address, id);
							available_workers[id] = worker;
							log.trace("W->Q register worker [%s]", id);
						}
					}

					if(worker !is null)
					{
						zframe_t* frame = zmsg_last(msg);
						//											print_data_from_frame ("W -> Q data:", frame);

						byte* data = zframe_data(frame);
						if(memcmp(data, cast(char*) PPP_READY, 1) && memcmp(data, cast(char*) PPP_HEARTBEAT, 1))
						{
							log.trace("E: invalid message from worker [%s]", worker.identity);
							zmsg_dump(msg);
						} else
						{
							//					writeln ("W->Q R ", aaa, " ", worker.identity, " ", worker.expiry, " ", count_r);
							worker.expiry = zclock_time() + PPP_HEARTBEAT_INTERVAL * PPP_HEARTBEAT_LIVENESS * 10_000;
							count_r++;
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
							log.trace("пришел результат от воркера [%s], но таковой не числится занятым работой", id);
							worker = null;
						}
					} else
					{
						log.trace("пришел результат от воркера [%s], но таковой у нас не зарегистрирован", id);
					}

					if(worker !is null)
					// && worker.msg !is null)
					{
						// это сообщение с результатом работы от воркера, исходное сообщение находится в worker.msg
						zframe_t* result_frame = zmsg_last(msg);
						byte* data = zframe_data(result_frame);
						int data_size = zframe_size(result_frame);

						string client_address = zframe_strdup(worker.client_address);

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

						worker.expiry = zclock_time() + PPP_HEARTBEAT_INTERVAL * PPP_HEARTBEAT_LIVENESS * 10_000;
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

			int msg_size = zmsg_size(msg);

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
		//			log.trace ("count_in_pool=%d, tt=%d", count_in_pool, tt);
		//		if (tt > 1_000_000)
		if(now_time >= heartbeat_at)
		{
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

			// отправим HEARTBEAT незанятым работой воркерам
			foreach(worker; available_workers.values)
			{
				if(worker.isBisy == false)
				{
					zframe_send(&worker.address, backend, ZFRAME_REUSE + ZFRAME_MORE);
					zframe_send(&frame_heartbeat, backend, ZFRAME_REUSE);

					//					log.trace("send heartbeat to [%s]", worker.identity);

					count_h++;
				}
			}

			heartbeat_at = zclock_time() + PPP_HEARTBEAT_INTERVAL * 1000 * 10;
		}

		StopWatch sw;
		sw.start();

		s_workers_purge(available_workers, m);
		sw.stop();
		long tt = sw.peek().usecs;
		//						log.trace ("time s_workers_purge: %d", tt);						
		//		s_workers_purge(bisy_workers);		
	}

	//  When we're done, clean up properly
	foreach(worker; available_workers.values)
	{
		s_worker_destroy(&worker);
	}

	zctx_destroy(&ctx);
	return;
}

Worker* task_to_worker(zframe_t* address, zframe_t* data, Main m)
{
	byte* data_b = zframe_data(data);
	int data_b_size = zframe_size(data);

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
						s_pos = i;

						// до следующей [""] будет сама команда
						for(i = s_pos + 1; i < data_b_size; i++)
							if(*(data_b + i) == '"')
							{
								int e_pos = i + 1;

								foreach(cmd; cmd__modify_db)
								{
									if(*(data_b + i) == cmd[0] && e_pos - s_pos == cmd.length)
									{
										// вероятно это нужная нам команда

										int j = 1;
										// сравним остальные символы
										for(i = s_pos + 2; i < e_pos; i++)
										{
											if(cmd[j] != *(data_b + i))
												break;
											j++;
										}

										if(j == cmd.length)
										{
											printf("found command:");
											for(i = s_pos; i < e_pos; i++)
												printf("%c", *(data_b + i));
											printf("\n");

										}
									}

								}

								break;
							}

						break;
					}

				break;
			}

		// найдена команда изменяющая базу данных, выполним ее для всех воркеров
	}

	// выбрать свободного воркера
	Worker* worker = null;

	foreach(wrk; available_workers.values)
	{
		if(wrk.isBisy == false)
		{
			worker = wrk;
			break;
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
