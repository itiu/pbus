module load_info;

private import std.array: appender;
private import std.format;

private import core.thread;
private import std.stdio;

private import std.datetime;

public bool cinfo_exit = false;

//private string set_bar_color = "\x1B[31m"; //"\x1B[41m";
private string set_bar_color = "\x1B[41m";
private string set_text_color_green = "\x1B[32m";
private string set_text_color_blue = "\x1B[34m";
private string set_all_attribute_off = "\x1B[0m";
private string set_cursor_in_begin_string = "\x1B[0E";

synchronized class Statistic
{
    int count_prepared_message = 0;
    int count_waiting_message = 0;
    int registred_workers_count = 0;    
}

class LoadInfoThread: Thread
{
	Statistic delegate() get_statistic;

	this(Statistic delegate() _get_statistic)
	{
		get_statistic = _get_statistic;
		super(&run);
	}

	private:

		void run()
		{
			double sleep_time = 1;
			Thread.getThis().sleep(cast(int) (sleep_time * 10_000_000));

			int prev_count = 0;
			int prev_waiting_count = 0;

			while(!cinfo_exit)
			{
				sleep_time = 1;

				Statistic stat = get_statistic();

				int delta_count = stat.count_prepared_message - prev_count;
				int delta_waiting = stat.count_waiting_message - prev_waiting_count;

//				if(delta_count > 0 || delta_waiting > 0)
				{
//					int delta_idle_time = idle_time - prev_idle_time;
//					prev_idle_time = idle_time;
//					int delta_worked_time = worked_time - prev_worked_time;
//					prev_worked_time = worked_time;

					char[] now = cast(char[]) getNowAsString();
					now[10] = ' ';
					now.length = 19;
					
            		auto writer = appender!string();
			        formattedWrite(writer, "%s | prepared :%6d | Δ prepared :%4d | rejected :%4d | Δ rejected:%5d | workers:%3d", 
			            now, stat.count_prepared_message, delta_count, stat.count_waiting_message, delta_waiting, stat.registred_workers_count);
					int d_delta_count = cast(int)((cast(float)writer.data.length / cast(float)6000) * delta_count + 1);
					writeln(set_bar_color, writer.data[0..d_delta_count], set_all_attribute_off, writer.data[d_delta_count..$]);
				}

			//	printf ("#5\n");
				prev_count = stat.count_prepared_message;
				prev_waiting_count = stat.count_waiting_message;
				Thread.getThis().sleep(cast(int) (sleep_time * 10_000_000));
			}
			writeln("exit form thread cinfo");
		}
}

string getNowAsString()
{
      SysTime sysTime = Clock.currTime();
      return sysTime.toISOExtString();        
}       
