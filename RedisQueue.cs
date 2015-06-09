using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace csharp_queue
{
	/// <summary>
	/// simple c# redis queue
	/// </summary>
	public class RedisQueue
	{
		IDatabase Db;
		public string AppPrefix = "ncsq";

		private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
		

		/// <summary>
		/// initialize RedisQueue with a existing redis connection
		/// </summary>
		/// <param name="db">Stackexchange.Redis ConnectionMultiplexer</param>
		public RedisQueue(IDatabase db)
		{
			Db = db;
		}

		/// <summary>
		/// initialize RedisQueue with a new redis connection
		/// </summary>
		public RedisQueue()
		{
			ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
			Db = redis.GetDatabase();
		}

		/// <summary>
		/// add a new job asynchronically
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <param name="job">job to add</param>
		/// <param name="user_id">User identifier.</param>
		/// <param name="priority">Priority (lower is faster, between 1 and 5)</param>
		public async Task EnQueueAsync(string queue, dynamic job, long user_id, short priority = 3)
		{
			if (priority < 1 || priority > 5) {
				priority = 3;
			}

			RedisValue job_json = new RedisValue();

			long job_id = await Db.StringIncrementAsync (AppPrefix + ":" + queue + ":id");

			Dictionary<string, dynamic> job_data = new Dictionary<string, dynamic> ();

			job_data ["data"] = job;
			job_data ["user_id"] = user_id;
			job_data ["priority"] = priority;
			job_data ["id"] = job_id;

			job_json = JsonConvert.SerializeObject (job_data);

			long score = GetCurrentUnixTimestampMillis() + (priority * 1000000000000000);

			var add_job = Db.StringSetAsync (AppPrefix + ":" + queue + ":jobs:" + job_id.ToString (), job_json);
			var add_job_user = Db.SortedSetAddAsync (AppPrefix + ":" + queue + ":user:" + user_id.ToString (), job_id, score);
			var add_job_queue = Db.SortedSetAddAsync (AppPrefix + ":" + queue + ":queue", job_id, score);

			await add_job;
			await add_job_user;
			await add_job_queue;
		}

		/// <summary>
		/// remove a specified amount of jobs from the queue
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <param name="limit">number of jobs to remove</param>
		/// <returns></returns>
		public async Task<List<dynamic>> DeQueueAsync(string queue, int limit = 1)
		{
			ITransaction multi = Db.CreateTransaction();

			Task<RedisValue[]> awaitJobIDs = multi.SortedSetRangeByRankAsync (AppPrefix + ":" + queue + ":queue", 0, limit - 1);
			multi.SortedSetRemoveRangeByRankAsync (AppPrefix + ":" + queue + ":queue", 0, limit - 1);

			bool committed = multi.Execute();

			if (committed == false)
			{
				throw new IndexOutOfRangeException();
			}

			RedisValue[] JobIDsTemp = await awaitJobIDs;
			RedisKey[] JobIDs = new RedisKey[JobIDsTemp.Length];

			int i = 0;
			foreach(RedisValue j in JobIDsTemp) {
				RedisKey key = AppPrefix + ":" + queue + ":jobs:" + (string)j;
				JobIDs [i] = key;
				i++;
			}

			ITransaction multi2 = Db.CreateTransaction ();

			Task<RedisValue[]> awaitJobs = multi2.StringGetAsync(JobIDs);
			multi2.KeyDeleteAsync (JobIDs, CommandFlags.FireAndForget);

			try {
				bool committed2 = multi2.Execute();

				if (committed2 == false) {
					throw new IndexOutOfRangeException();
				}

				RedisValue[] jobs = await awaitJobs;

				List<dynamic> returnJobs = new List<dynamic> ();

				foreach(RedisValue job in jobs) {
					dynamic job_parsed = JsonConvert.DeserializeObject(job);
					Db.SortedSetRemoveAsync(AppPrefix + ":" + queue + ":user:" + (string)job_parsed["user_id"], (string)job_parsed["id"], CommandFlags.FireAndForget);
					returnJobs.Add (job_parsed["data"]);
				}

				return returnJobs;
			}
			catch (RedisServerException e) {
				return new List<dynamic> ();
			}
		}

		public async Task<long> CountJobsAsync(string queue) {
			return await Db.SortedSetLengthAsync (AppPrefix + ":" + queue + ":queue");
		}

		public async Task<long?> getPosition(string queue, long user_id) {
			RedisValue[] JobID = await Db.SortedSetRangeByRankAsync (AppPrefix + ":" + queue + ":user:" + user_id.ToString (), 0, 0);

			if (JobID.Length < 1) {
				return null;
			}

			long? position = await Db.SortedSetRankAsync (AppPrefix + ":" + queue + ":queue", JobID[0]);

			if (position == null) {
				return null;
			}
			else {
				return position + 1;
			}
		}


		private static long GetCurrentUnixTimestampMillis()
		{
			return (long) (DateTime.UtcNow - UnixEpoch).TotalMilliseconds;
		}
	}
}