import json
import sys
import argparse
import sqlite3

def main():
    ap = argparse.ArgumentParser(description='Parse Twitch chatlogs')
    sp = ap.add_subparsers(dest='sub')
    p_crs = sp.add_parser('createchannel')
    p_crs.add_argument('name')
    p_crs.add_argument('id', type=int)

    p_ul = sp.add_parser('parsetopspam')
    p_ul.add_argument('file')

    p_gts = sp.add_parser('gettopspam')
    p_gts.add_argument("channel_id")
    p_gts.add_argument("stream_id")

    p_full = sp.add_parser("storechatlog")
    p_full.add_argument('file')

    p_qr = sp.add_parser("querychatlog")
    p_qr.add_argument("filters",nargs="+")

    pa = ap.parse_args()

    if pa.sub == "createchannel":
        #print("creating channel")
        conn = sqlite3.connect('twitch.db')
        c = conn.cursor()
        #c.execute('drop table if exists channels')
        c.execute('''CREATE TABLE if not exists channels
                    (channel_id integer primary key, channel_name text)''')

        q = "INSERT INTO CHANNELS VALUES ({},'{}')".format(pa.id, pa.name)
        c.execute(q)
        conn.commit()
        for r in c.execute("select * from channels where channel_id = {}".format(pa.id)):
            print(r)

        conn.close()        
    elif pa.sub == "parsetopspam":
        #print("generating summary for twitch chat log in {}".format(pa.file))
        counts = {}
        u_counts={}
        with open(pa.file) as f:
            j = json.load(f)

            cs = j['comments']

            fc = cs[0]
            channel_id = fc["channel_id"]
            stream_id = fc["content_id"]

            for c in cs:
                #fs = c["message"]["fragments"]
                user = c["commenter"]["display_name"]
                #for f in fs:
                #    t = f["text"]
                #    cnt = counts.get(t, 0)
                #    counts[t] = cnt+1

                #    ucnt = u_counts.get(t, set())
                #    ucnt.add(user)
                b = c["message"]["body"]
                cnt = counts.get(b, 0)
                counts[b] = cnt+1
                ucnt = u_counts.get(b, set())
                ucnt.add(user)
                u_counts[b] =ucnt

        conn = sqlite3.connect('twitch.db')
        c = conn.cursor()
        c.execute('PRAGMA foreign_keys = ON;')
        c.execute('create table if not exists top_spam (channel_id integer NOT NULL, stream_id integer NOT NULL, spam_text string, spam_occurrences integer, spam_user_count integer, FOREIGN KEY(channel_id) REFERENCES channels(channel_id))')
        c.execute('delete from top_spam where channel_id = ? and stream_id = ?', (channel_id, stream_id))
        conn.commit()

        s_counts = sorted(counts.items(), key=lambda kv:kv[1], reverse=True)
        count = 0
        for i,(k,v) in enumerate(s_counts):
            uc = len(u_counts[k])
            #print(i,k,v,uc)
            #should probably parameterize "10"
            if v > 10:
                c.execute('insert into top_spam values(?,?,?,?,?)', (channel_id, stream_id, k, v, uc))
                count+=1
            #if i > 19: break
        conn.commit()   
        conn.close()
        print("inserted {} top spam records for stream {} on channel {}".format(count, stream_id, channel_id))
    elif pa.sub == "gettopspam":
        #print("getting top spam") 
        conn = sqlite3.connect('twitch.db')
        c = conn.cursor()
        out = []
        for r in c.execute(("select * from top_spam where channel_id = {} and stream_id = " + pa.stream_id + " order by spam_occurrences desc, spam_user_count desc, spam_text").format(pa.channel_id)):
            out.append({"spam_text": r[2], "occurrences": r[3], "user_count":r[4]})
            #print(r)
        conn.close()
        print(json.dumps(out, sort_keys=True))
    elif pa.sub == "storechatlog":
        #print('storing raw logs in table')
        with open (pa.file) as f:
            j = json.load(f)

            cs = j['comments']

            fc = cs[0]
            channel_id = fc["channel_id"]
            stream_id = fc["content_id"]

            conn = sqlite3.connect('twitch.db')
            c = conn.cursor()
            #c.execute("drop table chat_log")
            c.execute('create table if not exists chat_log (channel_id integer NOT NULL, stream_id integer NOT NULL, text string, user string, chat_time datetime, offset int, FOREIGN KEY(channel_id) REFERENCES channels(channel_id))')
            c.execute('delete from chat_log where channel_id = ? and stream_id = ?', (channel_id, stream_id))
            
            for cmnt in cs:
                c.execute("insert into chat_log VALUES (?,?,?,?,?,?)", (channel_id, stream_id, cmnt["message"]["body"], cmnt["commenter"]["display_name"], cmnt["created_at"], cmnt["content_offset_seconds"]))
            
            conn.commit()
            conn.close()

            print("inserted {} records to chat log for stream {} on channel {}".format(len(cs), stream_id, channel_id))
    elif pa.sub == 'querychatlog':
        strcols = ['text', 'user']
        q = "select * from chat_log " 
        if len(pa.filters)> 0:
            q = q + "where "
        for f in pa.filters:
            fspos = f.index(' ')
            lspos = f.rindex(' ')
            col = f[0:fspos]
            val = f[lspos+1:]
            op = f[fspos+1:lspos]
            #print( col, val, op)
            fragment = col
            if op == "eq":
                fragment = fragment + " = "
            elif op == "gt":
                fragment = fragment + " > "
            elif op == "lt":
                fragment = fragment + " < "
            elif op == "gteq":
                fragment = fragment + " >= "
            elif op == "lteq":
                fragment = fragment + " <= "            
            elif op == "like":
                fragment = fragment + " like "

            if col in strcols:
                fragment = fragment + "'" + val + "' AND "
            else:
                fragment = fragment + val + ' AND '

            q = q + fragment
        
        if len(pa.filters) > 0:
            q = q[:-4]

        q = q + " order by chat_time"

        conn = sqlite3.connect('twitch.db')
        c = conn.cursor()
        out = []
        rows = c.execute(q)
        names = []
        for d in c.description:
            names.append(d[0])
        for r in rows:
            #print(r)
            j = {}
            for i,n in enumerate(names):
                j[n] = r[i]
            out.append(j)
        conn.close()
        print(json.dumps(out, sort_keys=True))


                


        
  
if __name__== "__main__":
    main()