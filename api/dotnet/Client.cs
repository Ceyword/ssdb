using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
 
using System.Net;
using System.Net.Sockets;
using System.IO;

namespace ssdb
{
	public class Client
	{
		private Link link;
		private string resp_code;
		private bool batch_queries { get; set; }
		private List<Task> query_list { get; set; }

		public Client(string host, int port)
		{
			link = new Link(host, port);
		}
		///<summary>
		///Sample Usage:
		/// 
		///Client Uc = new Client("127.0.0.1", 8888);
		///
		///Uc.BeginBatch();
		///
		///Uc.Batch(Uc.hsetAsync("delehp", "a", 100.ToString()));
		///Uc.Batch(Uc.zincrAsync("delezp", "b", 10));
		///Uc.Batch(Uc.zsetAsync("delezp", "c", 123));
		///
		///Task<string> a = Uc.hgetAsync("delehp", "a");
		///Task<long?> b = Uc.zgetAsync("delezp", "b");
		///Task<KeyValuePair<string, long>[]> c = Uc.multi_zgetAsync("delezp", new string[2]{"b","c"});
		///
		///Uc.Batch(new Task[3]{a,b,c});
		///
		///bool result = await Uc.ExecuteBatch();
		///
		///Uc.close();
		///
		///if (result == true) {
		///List<string[]> Received = new List<string[]>();
		///		Received.Add(new string[2]{"a",a.Result});
		///		Received.Add(new string[2] { "b solo", b.Result.ToString() });
		///		Received.Add(new string[2] { c.Result[0].Key, (c.Result[0].Value).ToString() });
		///		Received.Add(new string[2] { c.Result[1].Key, (c.Result[1].Value).ToString() });
		///		return Received;
		///} else {
		///		return null;
		///}
		/// </summary>
		public void BeginBatch()
		{
			batch_queries = true;
			query_list = new List<Task>();
		}

		public void Batch(Task query)
		{
			if (batch_queries == true)
			{
				query_list.Add(query);
			}
			else
			{
				throw new Exception("Initiate Batching by calling 'BeginBatch()' first");
			}
		}

		public void Batch(Task[] query)
		{
			if (batch_queries == true)
			{
				query_list.AddRange(query);
			}
			else
			{
				throw new Exception("Initiate Batching by calling 'BeginBatch()' first");
			}
		}

		public void Batch(List<Task> query)
		{
			if (batch_queries == true)
			{
				query_list.AddRange(query);
			}
			else
			{
				throw new Exception("Initiate Batching by calling 'BeginBatch()' first");
			}
		}

		public async Task<bool> ExecuteBatch()
		{
			try
			{

				if (batch_queries == true && query_list.Count > 0)
				{
					await Task.WhenAll( query_list);
                    return true;
				}
				else
				{
					return false;
				}
			}
			catch (Exception ex)
			{
				throw new Exception(ex.Message);
			}
			finally
			{
				batch_queries = false;
				query_list = new List<Task>();
			}
		}

		~Client()
		{
			this.close();
		}

		public void close()
		{
			link.close();
		}

		
		public List<byte[]> request(string cmd, params string[] args)
		{
			return link.request(cmd, args);
		}
		public async Task<List<byte[]>> requestAsync(string cmd, params string[] args)
		{
			return await link.requestAsync(cmd, args);
		}
		 
		
		public List<byte[]> request(string cmd, params byte[][] args)
		{
			return link.request(cmd, args);
		}
		public async Task<List<byte[]>> requestAsync(string cmd, params byte[][] args)
		{
			return await link.requestAsync(cmd, args);
		}

		
		public List<byte[]> request(List<byte[]> req)
		{
			return link.request(req);
		}
		public async Task<List<byte[]>> requestAsync(List<byte[]> req)
		{
			return await link.requestAsync(req);
		}
		

		private void assert_ok()
		{
			if (resp_code != "ok")
			{
				throw new Exception(resp_code);
			}
		}

		private byte[] _bytes(string s)
		{
			return Encoding.Default.GetBytes(s);
		}

		private string _string(byte[] bs)
		{
			return Encoding.Default.GetString(bs);
		}

		private KeyValuePair<string, byte[]>[] parse_scan_resp(List<byte[]> resp)
		{
			resp_code = _string(resp[0]);
			this.assert_ok();

			int size = (resp.Count - 1) / 2;
			KeyValuePair<string, byte[]>[] kvs = new KeyValuePair<string, byte[]>[size];
			for (int i = 0; i < size; i += 1)
			{
				string key = _string(resp[i * 2 + 1]);
				byte[] val = resp[i * 2 + 2];
				kvs[i] = new KeyValuePair<string, byte[]>(key, val);
			}
			return kvs;
		}

		private string[] parse_scan_resp_arr(List<byte[]> resp)
		{
			resp_code = _string(resp[0]);
			this.assert_ok();

			int size = (resp.Count - 1);
			string[] arr = new string[size];
			int i = 0;
			while (i < size)
			{
				arr[i] = _string(resp[i + 1]);
				i += 1;
			}
			return arr;
		}

		/***** kv *****/
		 
		
		public bool exists(byte[] key)
		{
			List<byte[]> resp = request("exists", key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return (_string(resp[1]) == "1" ? true : false);
		}
		public async Task<bool> existsAsync(byte[] key)
		{
			List<byte[]> resp = await requestAsync("exists", key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return (_string(resp[1]) == "1" ? true : false);
		}
		 
		
		public bool exists(string key)
		{
			return this.exists(_bytes(key));
		}
		public async Task<bool> existsAsync(string key)
		{
			return await this.existsAsync(_bytes(key));
		}
		         
		
		public void set(byte[] key, byte[] val)
		{
			List<byte[]> resp = request("set", key, val);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task setAsync(byte[] key, byte[] val)
		{
			List<byte[]> resp = await requestAsync("set", key, val );
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		 

		public void set(string key, string val)
		{
			this.set(_bytes(key), _bytes(val));
		}
		public async Task setAsync(string key, string val)
		{
			await this.setAsync(_bytes(key), _bytes(val));
		}

		/// <summary>
		///
		/// </summary>
		/// <param name="key"></param>
		/// <param name="val"></param>
		/// <returns>returns true if name.key is found, otherwise returns false.</returns>
		public bool get(byte[] key, out byte[] val)
		{
			val = null;
			List<byte[]> resp = request("get", key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			val = resp[1];
			return true;
		}
		public async Task<byte[]> getAsync(byte[] key)
		{
			List<byte[]> resp = await requestAsync("get", key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return null;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return resp[1];
		}

		
		public bool get(string key, out byte[] val)
		{
			return this.get(_bytes(key), out val);
		}
		public bool get(string key, out string val)
		{
			val = null;
			byte[] bs;
			if (!this.get(key, out bs))
			{
				return false;
			}
			val = _string(bs);
			return true;
		}
		public async Task<string> getAsync(string key)
		{
			byte[] bs = await getAsync(_bytes(key));
			if (bs == null)
			{
				return null;
			}
			else
			{
				return _string(bs);
			}
		}
		 

		public void del(byte[] key)
		{
			List<byte[]> resp = request("del", key);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task delAsync(byte[] key)
		{
			List<byte[]> resp = await requestAsync("del", key);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}


		public void del(string key)
		{
			this.del(_bytes(key));
		}
		public async Task delAsync(string key)
		{
			await this.delAsync(_bytes(key));
		}
		
				
		public KeyValuePair<string, byte[]>[] scan(string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp = request("scan", key_start, key_end, limit.ToString());
			return parse_scan_resp(resp);
		}
		public async Task<KeyValuePair<string, byte[]>[]> scanAsync(string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp = await requestAsync("scan", key_start, key_end, limit.ToString());
			return parse_scan_resp(resp);
		}

		 
		public KeyValuePair<string, byte[]>[] rscan(string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp = request("rscan", key_start, key_end, limit.ToString());
			return parse_scan_resp(resp);
		}
		public async Task<KeyValuePair<string, byte[]>[]> rscanAsync(string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp = await requestAsync("rscan", key_start, key_end, limit.ToString());
			return parse_scan_resp(resp);
		}


		public void multi_set(KeyValuePair<byte[], byte[]>[] kvs)
		{
			byte[][] req = new byte[kvs.Length * 2][];
			for (int i = 0; i < kvs.Length; i++)
			{
				req[2 * i] = kvs[i].Key;
				req[(2 * i) + 1] = kvs[i].Value;

			}
			List<byte[]> resp = request("multi_set", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task multi_setAsync(KeyValuePair<byte[], byte[]>[] kvs)
		{
			byte[][] req = new byte[(kvs.Length * 2)][];

			for (int i = 0; i < kvs.Length; i++)
			{
				req[2 * i] = kvs[i].Key;
				req[(2 * i) + 1] = kvs[i].Value;
			}
			List<byte[]> resp = await requestAsync("multi_set", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		

		public void multi_set(KeyValuePair<string, string>[] kvs)
		{
			KeyValuePair<byte[], byte[]>[] req = new KeyValuePair<byte[], byte[]>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
			this.multi_set(req);
		}
		public async Task multi_setAsync(KeyValuePair<string, string>[] kvs)
		{
			KeyValuePair<byte[], byte[]>[] req = new KeyValuePair<byte[], byte[]>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
			await this.multi_setAsync(req);
		}
		

		public void multi_set(List<KeyValuePair<string, string>> kvs)
		{
			KeyValuePair<byte[], byte[]>[] req = new KeyValuePair<byte[], byte[]>[kvs.Count];
			for (int i = 0; i < kvs.Count; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
			this.multi_set(req);
		}
		public async Task multi_setAsync(List<KeyValuePair<string, string>> kvs)
		{
			KeyValuePair<byte[], byte[]>[] req = new KeyValuePair<byte[], byte[]>[kvs.Count];
			for (int i = 0; i < kvs.Count; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
			await this.multi_setAsync(req);
		}
		 
		
		public void multi_del(byte[][] keys)
		{
			List<byte[]> resp = request("multi_del", keys);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task multi_delAsync(byte[][] keys)
		{
			List<byte[]> resp = await requestAsync("multi_del", keys);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}


		public void multi_del(string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			this.multi_del(req);
		}
		public async Task multi_delAsync(string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			await this.multi_delAsync(req);
		}


		public KeyValuePair<string, byte[]>[] multi_get(byte[][] keys)
		{
			List<byte[]> resp = request("multi_get", keys);
			KeyValuePair<string, byte[]>[] ret = parse_scan_resp(resp);

			return ret;
		}
		public async Task<KeyValuePair<string, byte[]>[]> multi_getAsync(byte[][] keys)
		{
			List<byte[]> resp = await requestAsync("multi_get", keys);
			KeyValuePair<string, byte[]>[] ret = parse_scan_resp(resp);
			return ret;
		}
		

		public KeyValuePair<string, byte[]>[] multi_get(string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			return this.multi_get(req);
		}
		public async Task<KeyValuePair<string, byte[]>[]> multi_getAsync(string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			return await this.multi_getAsync(req);
		}
		
		/***** hash *****/
				
		public void hset(byte[] name, byte[] key, byte[] val)
		{
			List<byte[]> resp = request("hset", name, key, val);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task hsetAsync(byte[] name, byte[] key, byte[] val)
		{
			List<byte[]> resp = await requestAsync("hset", name, key, val);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		
		
		public void hset(string name, string key, byte[] val)
		{
			this.hset(_bytes(name), _bytes(key), val);
		}
		public async Task hsetAsync(string name, string key, byte[] val)
		{
			await this.hsetAsync(_bytes(name), _bytes(key), val);
		}
		
		
		public void hset(string name, string key, string val)
		{
			this.hset(_bytes(name), _bytes(key), _bytes(val));
		}
		public async Task hsetAsync(string name, string key, string val)
		{
			await this.hsetAsync(_bytes(name), _bytes(key), _bytes(val));
		}


		public Int64 hincr(byte[] name, byte[] key, Int64 increment)
		{
			List<byte[]> resp = request("hincr", name, key, _bytes(increment.ToString()));
			resp_code = _string(resp[0]);
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}
		public async Task<long> hincrAsync(byte[] name, byte[] key, Int64 increment)
		{
			List<byte[]> resp = await requestAsync("hincr", name, key, _bytes(increment.ToString()));
			resp_code = _string(resp[0]);
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}


		public Int64 hincr(string name, string key, Int64 increment)
		{
			return this.hincr(_bytes(name), _bytes(key), increment);
		}
		public async Task<long> hincrAsync(string name, string key, Int64 increment)
		{
			return await this.hincrAsync(_bytes(name), _bytes(key), increment);
		}
  		
		/// <summary>
		///
		/// </summary>
		/// <param name="name"></param>
		/// <param name="key"></param>
		/// <param name="val"></param>
		/// <returns>returns true if name.key is found, otherwise returns false.</returns>
		public bool hget(byte[] name, byte[] key, out byte[] val)
		{
			val = null;
			List<byte[]> resp = request("hget", name, key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			val = resp[1];
			return true;
		}
		public async Task<byte[]> hgetAsync(byte[] name, byte[] key)
		{
			List<byte[]> resp = await requestAsync("hget", name, key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return null;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return resp[1];
		}
		
		
		public bool hget(string name, string key, out byte[] val)
		{
			return this.hget(_bytes(name), _bytes(key), out val);
		}
		public bool hget(string name, string key, out string val)
		{
			val = null;
			byte[] bs;
			if (!this.hget(name, key, out bs))
			{
				return false;
			}
			val = _string(bs);
			return true;
		}
		public async Task<string> hgetAsync(string name, string key)
		{
			byte[] bs = await hgetAsync(_bytes(name), _bytes(key));
			if (bs==null)
			{
				return null;
			}
			else
			{
			return _string(bs);
			}
		}
		

		public string[] hkeys(string name, string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp = request("hkeys", name, key_start, key_end, limit.ToString());
			return parse_scan_resp_arr(resp);
		}
		public async Task<string[]> hkeysAsync(string name, string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp = await requestAsync("hkeys", name, key_start, key_end, limit.ToString());
			return parse_scan_resp_arr(resp);
		}
		
		
		public string[] hkeysAll(string name)
		{
			List<byte[]> resp = request("hkeys", name, "", "", Int32.MaxValue.ToString());
			return parse_scan_resp_arr(resp);
		}
		public async Task<string[]> hkeysAllAsync(string name)
		{
			List<byte[]> resp = await requestAsync("hkeys", name, "", "", Int32.MaxValue.ToString());
			return parse_scan_resp_arr(resp);
		}
		 

		public void hdel(byte[] name, byte[] key)
		{
			List<byte[]> resp = request("hdel", name, key);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task hdelAsync(byte[] name, byte[] key)
		{
			List<byte[]> resp =	await requestAsync("hdel", name, key);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}


		public void hdel(string name, string key)
		{
			this.hdel(_bytes(name), _bytes(key));
		}
		public async Task hdelAsync(string name, string key)
		{
			await this.hdelAsync(_bytes(name), _bytes(key));
		}


		public bool hexists(byte[] name, byte[] key)
		{
			List<byte[]> resp = request("hexists", name, key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return (_string(resp[1]) == "1" ? true : false);

		}
		public async Task<bool> hexistsAsync(byte[] name, byte[] key)
		{
			List<byte[]> resp = await requestAsync("hexists", name, key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return (_string(resp[1]) == "1" ? true : false);

		}
		

		public bool hexists(string name, string key)
		{
			return this.hexists(_bytes(name), _bytes(key));
		}
		public async Task<bool> hexistsAsync(string name, string key)
		{
			return await this.hexistsAsync(_bytes(name), _bytes(key));
		}
		

		public Int64 hsize(byte[] name)
		{
			List<byte[]> resp = request("hsize", name);
			resp_code = _string(resp[0]);
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}
		public async Task<Int64> hsizeAsync(byte[] name)
		{
			List<byte[]> resp =	await requestAsync("hsize", name);
			resp_code = _string(resp[0]);
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}


		public Int64 hsize(string name)
		{
			return this.hsize(_bytes(name));
		}
		public async Task<Int64> hsizeAsync(string name)
		{
			return await this.hsizeAsync(_bytes(name));
		}


		public KeyValuePair<string, byte[]>[] hscan(string name, string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp = request("hscan", name, key_start, key_end, limit.ToString());
			return parse_scan_resp(resp);
		}
		public async Task<KeyValuePair<string, byte[]>[]> hscanAsync(string name, string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp =	await requestAsync("hscan", name, key_start, key_end, limit.ToString());
			return parse_scan_resp(resp);
		}


		public KeyValuePair<string, byte[]>[] hrscan(string name, string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp = request("hrscan", name, key_start, key_end, limit.ToString());
			return parse_scan_resp(resp);
		}
		public async Task<KeyValuePair<string, byte[]>[]> hrscanAsync(string name, string key_start, string key_end, Int64 limit)
		{
			List<byte[]> resp =	await requestAsync("hrscan", name, key_start, key_end, limit.ToString());
			return parse_scan_resp(resp);
		}

		
		public void multi_hset(byte[] name, KeyValuePair<byte[], byte[]>[] kvs)
		{
			byte[][] req = new byte[(kvs.Length * 2) + 1][];
			req[0] = name;
			for (int i = 0; i < kvs.Length; i++)
			{
				req[(2 * i) + 1] = kvs[i].Key;
				req[(2 * i) + 2] = kvs[i].Value;
			}
			List<byte[]> resp = request("multi_hset", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task multi_hsetAsync(byte[] name, KeyValuePair<byte[], byte[]>[] kvs)
		{
			byte[][] req = new byte[(kvs.Length * 2) + 1][];
			req[0] = name;
			for (int i = 0; i < kvs.Length; i++)
			{
				req[(2 * i) + 1] = kvs[i].Key;
				req[(2 * i) + 2] = kvs[i].Value;
			}
			List<byte[]> resp = await requestAsync("multi_hset", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		

		public void multi_hset(string name, KeyValuePair<string, string>[] kvs)
		{
			KeyValuePair<byte[], byte[]>[] req = new KeyValuePair<byte[], byte[]>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
			this.multi_hset(_bytes(name), req);
		}
		public async Task multi_hsetAsync(string name, KeyValuePair<string, string>[] kvs)
		{
			KeyValuePair<byte[], byte[]>[] req = new KeyValuePair<byte[], byte[]>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
			await this.multi_hsetAsync(_bytes(name), req);
		}

		
		public void multi_hset(string name, List<KeyValuePair<string, string>> kvs)
		{
			KeyValuePair<byte[], byte[]>[] req = new KeyValuePair<byte[], byte[]>[kvs.Count];
			for (int i = 0; i < kvs.Count; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
			this.multi_hset(_bytes(name), req);
		}
		public async Task multi_hsetAsync(string name, List<KeyValuePair<string, string>> kvs)
		{
			KeyValuePair<byte[], byte[]>[] req = new KeyValuePair<byte[], byte[]>[kvs.Count];
			for (int i = 0; i < kvs.Count; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
			await this.multi_hsetAsync(_bytes(name), req);
		}
		

		public void multi_hdel(byte[] name, byte[][] keys)
		{
			byte[][] req = new byte[keys.Length + 1][];
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			List<byte[]> resp = request("multi_hdel", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task multi_hdelAsync(byte[] name, byte[][] keys)
		{
			byte[][] req = new byte[keys.Length + 1][];
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			List<byte[]> resp = await  requestAsync("multi_hdel", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		

		public void multi_hdel(string name, string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			this.multi_hdel(_bytes(name), req);
		}
		public async Task multi_hdelAsync(string name, string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			 await this.multi_hdelAsync(_bytes(name), req);
		}
		

		public KeyValuePair<string, byte[]>[] multi_hget(byte[] name, byte[][] keys)
		{
			byte[][] req = new byte[keys.Length + 1][];
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			List<byte[]> resp = request("multi_hget", req);
			KeyValuePair<string, byte[]>[] ret = parse_scan_resp(resp);

			return ret;
		}
		public async Task<KeyValuePair<string, byte[]>[]> multi_hgetAsync(byte[] name, byte[][] keys)
		{
			byte[][] req = new byte[keys.Length + 1][];
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i]; 
			}
			List<byte[]> resp = await requestAsync("multi_hget", req);
			KeyValuePair<string, byte[]>[] ret = parse_scan_resp(resp);

			return ret;
		}
		

		public KeyValuePair<string, byte[]>[] multi_hget(string name, string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			return this.multi_hget(_bytes(name), req);
		}
		public async Task<KeyValuePair<string, byte[]>[]> multi_hgetAsync(string name, string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			return await this.multi_hgetAsync(_bytes(name), req);
		}

		/***** zset *****/

		public void zset(byte[] name, byte[] key, Int64 score)
		{
			List<byte[]> resp = request("zset", name, key, _bytes(score.ToString()));
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task zsetAsync(byte[] name, byte[] key, Int64 score)
		{
			List<byte[]> resp = await requestAsync("zset", name, key, _bytes(score.ToString()));
			resp_code = _string(resp[0]);
			this.assert_ok();
		}


		public void zset(string name, string key, Int64 score)
		{
			this.zset(_bytes(name), _bytes(key), score);
		}
		public async Task zsetAsync(string name, string key, Int64 score)
		{
			await this.zsetAsync(_bytes(name), _bytes(key), score);
		}
		

		public Int64 zincr(byte[] name, byte[] key, Int64 increment)
		{
			List<byte[]> resp = request("zincr", name, key, _bytes(increment.ToString()));
			resp_code = _string(resp[0]);
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}
		public async Task<long> zincrAsync(byte[] name, byte[] key, Int64 increment)
		{
			List<byte[]> resp = await requestAsync("zincr", name, key, _bytes(increment.ToString()));
			resp_code = _string(resp[0]);
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}

		
		public Int64 zincr(string name, string key, Int64 increment)
		{
			return this.zincr(_bytes(name), _bytes(key), increment);
		}
		public async Task<long> zincrAsync(string name, string key, Int64 increment)
		{
			return await this.zincrAsync(_bytes(name), _bytes(key), increment);
		}

		/// <summary>
		///
		/// </summary>
		/// <param name="name"></param>
		/// <param name="key"></param>
		/// <param name="score"></param>
		/// <returns>returns true if name.key is found, otherwise returns false.</returns>
		public bool zget(byte[] name, byte[] key, out Int64 score)
		{
			score = -1;
			List<byte[]> resp = request("zget", name, key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			score = Int64.Parse(_string(resp[1]));
			return true;
		}
		public async Task<Int64?> zgetAsync(byte[] name, byte[] key)
		{
			List<byte[]> resp = await requestAsync("zget", name, key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return null;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}


		public bool zget(string name, string key, out Int64 score)
		{
			return this.zget(_bytes(name), _bytes(key), out score);
		}
		public async Task<Int64?> zgetAsync(string name, string key)
		{
			return await this.zgetAsync(_bytes(name), _bytes(key));
		}


		public void zdel(byte[] name, byte[] key)
		{
			List<byte[]> resp = request("zdel", name, key);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task zdelAsync(byte[] name, byte[] key)
		{
			List<byte[]> resp = await requestAsync("zdel", name, key);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}


		public void zdel(string name, string key)
		{
			this.zdel(_bytes(name), _bytes(key));
		}
		public async Task zdelAsync(string name, string key)
		{
			await this.zdelAsync(_bytes(name), _bytes(key));
		}


		public Int64 zsize(byte[] name)
		{
			List<byte[]> resp = request("zsize", name);
			resp_code = _string(resp[0]);
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}
		public async Task<long> zsizeAsync(byte[] name)
		{
			List<byte[]> resp = await requestAsync("zsize", name);
			resp_code = _string(resp[0]);
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return Int64.Parse(_string(resp[1]));
		}


		public Int64 zsize(string name)
		{
			return this.zsize(_bytes(name));
		}
		public async Task<long> zsizeAsync(string name)
		{
			return await this.zsizeAsync(_bytes(name));
		}


		public bool zexists(byte[] name, byte[] key)
		{
			List<byte[]> resp = request("zexists", name, key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return (_string(resp[1]) == "1" ? true : false);

		}
		public async Task<bool> zexistsAsync(byte[] name, byte[] key)
		{
			List<byte[]> resp = await requestAsync("zexists", name, key);
			resp_code = _string(resp[0]);
			if (resp_code == "not_found")
			{
				return false;
			}
			this.assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return (_string(resp[1]) == "1" ? true : false);

		}
		

		public bool zexists(string name, string key)
		{
			return this.zexists(_bytes(name), _bytes(key));
		}
		public async Task<bool> zexistsAsync(string name, string key)
		{
			return await this.zexistsAsync(_bytes(name), _bytes(key));
		}

		
		public KeyValuePair<string, Int64>[] zrange(string name, Int32 offset, Int32 limit)
		{

			List<byte[]> resp = request("zrange", name, offset.ToString(), limit.ToString());
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}
		public async Task<KeyValuePair<string, long>[]> zrangeAsync(string name, Int32 offset, Int32 limit)
		{

			List<byte[]> resp = await requestAsync("zrange", name, offset.ToString(), limit.ToString());
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}
		

		public KeyValuePair<string, Int64>[] zrrange(string name, Int32 offset, Int32 limit)
		{

			List<byte[]> resp = request("zrrange", name, offset.ToString(), limit.ToString());
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}
		public async Task<KeyValuePair<string, long>[]> zrrangeAsync(string name, Int32 offset, Int32 limit)
		{

			List<byte[]> resp = await requestAsync("zrrange", name, offset.ToString(), limit.ToString());
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}


		public KeyValuePair<string, Int64>[] zscan(string name, string key_start, Int64 score_start, Int64 score_end, Int64 limit)
		{
			string score_s = "";
			string score_e = "";
			if (score_start != Int64.MinValue)
			{
				score_s = score_start.ToString();
			}
			if (score_end != Int64.MaxValue)
			{
				score_e = score_end.ToString();
			}
			List<byte[]> resp = request("zscan", name, key_start, score_s, score_e, limit.ToString());
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}
		public async Task<KeyValuePair<string, long>[]> zscanAsync(string name, string key_start, Int64 score_start, Int64 score_end, Int64 limit)
		{
			string score_s = "";
			string score_e = "";
			if (score_start != Int64.MinValue)
			{
				score_s = score_start.ToString();
			}
			if (score_end != Int64.MaxValue)
			{
				score_e = score_end.ToString();
			}
			List<byte[]> resp = await requestAsync("zscan", name, key_start, score_s, score_e, limit.ToString()); ;
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}

		
		public KeyValuePair<string, Int64>[] zrscan(string name, string key_start, Int64 score_start, Int64 score_end, Int64 limit)
		{
			string score_s = "";
			string score_e = "";
			if (score_start != Int64.MaxValue)
			{
				score_s = score_start.ToString();
			}
			if (score_end != Int64.MinValue)
			{
				score_e = score_end.ToString();
			}
			List<byte[]> resp = request("zrscan", name, key_start, score_s, score_e, limit.ToString());
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}
		public async Task<KeyValuePair<string, long>[]> zrscanAsync(string name, string key_start, Int64 score_start, Int64 score_end, Int64 limit)
		{
			string score_s = "";
			string score_e = "";
			if (score_start != Int64.MaxValue)
			{
				score_s = score_start.ToString();
			}
			if (score_end != Int64.MinValue)
			{
				score_e = score_end.ToString();
			}
			List<byte[]> resp = await requestAsync("zrscan", name, key_start, score_s, score_e, limit.ToString());
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}


		public void multi_zset(byte[] name, KeyValuePair<byte[], Int64>[] kvs)
		{
			byte[][] req = new byte[(kvs.Length * 2) + 1][];
			req[0] = name;
			for (int i = 0; i < kvs.Length; i++)
			{
				req[(2 * i) + 1] = kvs[i].Key;
				req[(2 * i) + 2] = _bytes(kvs[i].Value.ToString());
			}
			List<byte[]> resp = request("multi_zset", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task multi_zsetAsync(byte[] name, KeyValuePair<byte[], Int64>[] kvs)
		{
			byte[][] req = new byte[(kvs.Length * 2) + 1][];
			req[0] = name;
			for (int i = 0; i < kvs.Length; i++)
			{
				req[(2 * i) + 1] = kvs[i].Key;

				req[(2 * i) + 2] = _bytes(kvs[i].Value.ToString());
			}
			List<byte[]> resp = await requestAsync("multi_zset", req); ;
			resp_code = _string(resp[0]);
			this.assert_ok();
		}

		
		public void multi_zset(string name, KeyValuePair<string, Int64>[] kvs)
		{
			KeyValuePair<byte[], Int64>[] req = new KeyValuePair<byte[], Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				req[i] = new KeyValuePair<byte[], Int64>(_bytes(kvs[i].Key), kvs[i].Value);
			}
			this.multi_zset(_bytes(name), req);
		}
		public async Task multi_zsetAsync(string name, KeyValuePair<string, Int64>[] kvs)
		{
			KeyValuePair<byte[], Int64>[] req = new KeyValuePair<byte[], Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				req[i] = new KeyValuePair<byte[], Int64>(_bytes(kvs[i].Key), kvs[i].Value);
			}
			await this.multi_zsetAsync(_bytes(name), req);
		}

		
		public void multi_zset(string name, List<KeyValuePair<string, Int64>> kvs)
		{
			KeyValuePair<byte[], Int64>[] req = new KeyValuePair<byte[], Int64>[kvs.Count];
			for (int i = 0; i < kvs.Count; i++)
			{
				req[i] = new KeyValuePair<byte[], Int64>(_bytes(kvs[i].Key), kvs[i].Value);
			}
			this.multi_zset(_bytes(name), req);
		}
		public async Task multi_zsetAsync(string name, List<KeyValuePair<string, Int64>> kvs)
		{
			KeyValuePair<byte[], Int64>[] req = new KeyValuePair<byte[], Int64>[kvs.Count];
			for (int i = 0; i < kvs.Count; i++)
			{
				req[i] = new KeyValuePair<byte[], Int64>(_bytes(kvs[i].Key), kvs[i].Value);
			}
			await this.multi_zsetAsync(_bytes(name), req);;
		}

		
		public void multi_zdel(byte[] name, byte[][] keys)
		{
			byte[][] req = new byte[keys.Length + 1][];
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			List<byte[]> resp = request("multi_zdel", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		public async Task multi_zdelAsync(byte[] name, byte[][] keys)
		{
			byte[][] req = new byte[keys.Length + 1][];
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			List<byte[]> resp = await requestAsync("multi_zdel", req);
			resp_code = _string(resp[0]);
			this.assert_ok();
		}
		

		public void multi_zdel(string name, string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			this.multi_zdel(_bytes(name), req);
		}
		public async Task multi_zdelAsync(string name, string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			await this.multi_zdelAsync(_bytes(name), req);
		}
		

		public KeyValuePair<string, Int64>[] multi_zget(byte[] name, byte[][] keys)
		{
			byte[][] req = new byte[keys.Length + 1][];
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			List<byte[]> resp = request("multi_zget", req);
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}
		public async Task<KeyValuePair<string, long>[]> multi_zgetAsync(byte[] name, byte[][] keys)
		{
			byte[][] req = new byte[keys.Length + 1][] ;
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			List<byte[]> resp = await requestAsync("multi_zget", req);
			KeyValuePair<string, byte[]>[] kvs = parse_scan_resp(resp);
			KeyValuePair<string, Int64>[] ret = new KeyValuePair<string, Int64>[kvs.Length];
			for (int i = 0; i < kvs.Length; i++)
			{
				string key = kvs[i].Key;
				Int64 score = Int64.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, Int64>(key, score);
			}
			return ret;
		}
		

		public KeyValuePair<string, Int64>[] multi_zget(string name, string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			return this.multi_zget(_bytes(name), req);
		}
		public async Task<KeyValuePair<string, long>[]> multi_zgetAsync(string name, string[] keys)
		{
			byte[][] req = new byte[keys.Length][];
			for (int i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			return await this.multi_zgetAsync(_bytes(name), req);
		}


		

	}
}


