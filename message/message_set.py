#!/usr/bin/env python
#  -*- coding:UTF-8 -*-
# 向私信kafak队列中写入数据
# kafka队列配置信息从配置文件中读取
# 脚本参数：
# -c [configfile] 配置文件
# -i [inputfile] 输入数据,逗号分隔
#


from confluent_kafka import Producer
import sys, getopt, ConfigParser
import json

def loadfileName():
    argv = sys.argv[1:]
    configfile = ''
    inputfile = ''
    try:
        opts, args = getopt.getopt(argv, "c:i:")
    except getopt.GetoptError:
        print 'Usage: message_set.py -i [inputfile] -c [configfile]'
        sys.exit(2)
    if len(opts) != 2:
        print 'Usage: message_set.py -i [inputfile] -c [configfile]'
        sys.exit(2)
    for opt, agr in opts:
        if opt == '-i':
            inputfile = agr
        elif opt == '-c':
            configfile = agr
    return (inputfile, configfile)

def parse_args(filename):  
    cf = ConfigParser.ConfigParser()  
    cf.read(filename)  
       
    #线上kafka section  
    online = cf.options("online")  
    
    items = cf.items("online")        
    #read  
    _ip = cf.get("online","ip")  
    _port = cf.getint("online", "port")  
    _topic = cf.get("online", "topic")  
    return (_ip, _port, _topic) 

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' % (msg.topic(), msg.partition(), msg.offset()))

if __name__ == '__main__':
    inputFile, configFile = loadfileName()

    kafka_ip, kafka_port, kafka_topic = parse_args(configFile)

    f = open(inputFile, 'r')

    msglist = f.readlines()
    f.close()
    broker = '%s:%d' % (kafka_ip, kafka_port)
    conf = {'bootstrap.servers': broker}
    p = Producer(**conf)
    print "ddddd"
    #主流程，循环遍历每行文件内容，转换为json写入kafka
    for line in msglist:
        msgItem = line.split(',')
        
        outputDict = {}
        outputDict['source_uid'] = msgItem[0]
        outputDict['target_uid'] = msgItem[1]
        outputDict['mid'] = msgItem[2]
        outputDict['time'] = msgItem[3]
        outputDict['identity'] = msgItem[4]
        outputDict['action_code'] = msgItem[5]
        outputDict['current_time'] = msgItem[6].strip()
        msgJson = json.dumps(outputDict)
        print msgJson

        try:
            p.produce(kafka_topic, msgJson, callback=delivery_callback)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
        p.poll(0)
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
