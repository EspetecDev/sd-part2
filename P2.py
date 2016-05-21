"Author: Manel Hidalgo i Octavi Juan"

from pyactive.controller import init_host, serve_forever, start_controller, interval_host, later
from random import *
import os


class Sensor:
    _sync = {}
    _async = ['start', 'read_data', 'stopInterval']
    _parallel = []
    _ref = []

    def __init__(self, st, log):
        self.st = st
        self.log = log

    def start(self, filename):

        f = open(filename, 'r')  # We open the file in "read" mode
        self.data = []
        for line in f.readlines():
            self.data.append(line)

        f.close()
        self.index = 0
        self.index2 = 1
        self.readings = (len(self.data[0]) / 2)

        self.interval = interval_host(self.host, 1, self.read_data)

    def stopInterval(self):
        self.interval.set()
        self.host.shutdown()

    def read_data(self):
        self.log.printIteration(self.index2)
        self.index2 += 1

        for i in range(len(self.st)):
            self.st[i].input_data((self.data[i])[self.index])
        self.index += 2

        if (self.index == (self.readings * 2)):
            self.stopInterval()


class StreetLight:
    _sync = {}
    _async = ['switch_on', 'switch_off', 'input_data', 'set_queue']
    _parallel = []
    _ref = ['set_queue']

    def __init__(self, bd_id):
        self.id = -99
        self.light = False
        self.arraySl = ['0'] * 4
        self.bd_id = bd_id
        self.lamport = 0

    def switch_on(self):
        self.light = True

    def switch_off(self):
        self.light = False

    def set_queue(self, queue, id):
        self.queue = queue
        self.id = id

    def input_data(self, data):
        self.queue.receive_data(data, self.id, self.bd_id, ++self.lamport)


class Queue:
    _sync = {}
    _async = ['receive_data']
    _parallel = []
    _ref = []

    def __init__(self, serverArray, servers):
        self.serverArray = serverArray
        self.servers = servers
        self.next = randint(0, 1000) % self.servers

    def receive_data(self, data, id, bd_id, lamport):
        # Round Robin
        self.serverArray[self.next].receive_data(data, id, bd_id)
        self.next = randint(0, 1000) % self.servers




class Server:
    _sync = {}
    _async = ['receive_data']
    _parallel = []
    _ref = []

    def __init__(self, slArray, log, id, bdArray):
        self.slArray = slArray
        self.bdArray = bdArray

        self.log = log

        self.id = id
        self.bd_id = -1
        self.sl_id = -1



    def receive_data(self, data, id_sl, bd_id): #bd_id deberia desaparecer

        #self.bd_id = bd_id
        self.sl_id = id_sl

        for i in bdArray:
            i.voting(data, id_sl) # Envia la votacion a las bases de datos

        self.bdArray[self.bd_id].write_data(data, self.sl_id)
        dataArray = self.bdArray[self.bd_id].getData(self.sl_id)

        self.state = self.bdArray[self.bd_id].getState(self.sl_id)
        self.cont = self.bdArray[self.bd_id].getCont(self.sl_id)


        if dataArray[-1] == '1':
            if self.state == 0:
                self.slArray[self.sl_id].switch_on()
                self.log.printmsg(self.id, self.sl_id, self.bd_id, dataArray, 'ON')
                # self.state[self.sl_id] = 1
                self.bdArray[self.bd_id].setState(self.sl_id, 1)
                self.state = self.bdArray[self.bd_id].getState(self.sl_id)
            # self.cont[self.sl_id] = 0
                self.bdArray[self.bd_id].setCont(self.sl_id, 0)
                self.cont = self.bdArray[self.bd_id].getCont(self.sl_id)


        if dataArray[-1] == '0':
            if self.state == 1:
                self.bdArray[self.bd_id].setCont(self.sl_id, self.cont + 1)
                self.cont = self.bdArray[self.bd_id].getCont(self.sl_id)
                if self.cont >= 4:
                    self.slArray[self.sl_id].switch_off()
                    self.log.printmsg(self.id, self.sl_id, self.bd_id, dataArray, 'OFF')
                    self.bdArray[self.bd_id].setState(self.sl_id, 0)

        self.state = self.bdArray[self.bd_id].getState(self.sl_id)
        self.cont = self.bdArray[self.bd_id].getCont(self.sl_id)


class Bd:
    _sync = {'getData': 3, 'getState': 3, 'getCont': 3, 'voting': 3}
    _async = ['write_data', 'setState', 'setCont']
    _parallel = []
    _ref = []

    def __init__(self, sl_division, index, sl):
        self.sl_division = sl_division
        self.slList = []
        self.data = ['0'] * 4
        self.index = index
        self.streetLights = sl
        self.stateList = [0] * self.streetLights
        self.contList = [0] * self.streetLights
        self.lamportList = [0] * self.streetLights

        for i in range(sl_division):
            self.slList.append(self.data)

    def getState(self, sl_id):
        return self.stateList[sl_id]

    def setState(self, sl_id, state):
        self.stateList[sl_id] = state

    def getCont(self, sl_id):
        return self.contList[sl_id]

    def setCont(self, sl_id, cont):
        self.contList[sl_id] = cont


    def getData(self, sl_id):
        return self.slList[sl_id - self.index]

    def write_data(self, data, sl_id):

        self.slList[sl_id - self.index].append(data)

        if len(self.slList[sl_id - self.index]) == 5:
            self.slList[sl_id - self.index].remove(self.slList[sl_id - self.index][0])

    def voting(self, data, sl_id):
        if vote:
            return 


class Log:
    _sync = {}
    _async = ['printmsg', 'printIteration', 'debugState', 'debugData', 'debugCont','debugMSG']
    _parallel = []
    _ref = []

    def __init__(self):
        self.f = open('log.txt', 'w')

    def debugData(self, data, sl_id):
        print "[DEBUG] [SL]: %d -- Data: %s" %(sl_id, data)
    def debugState(self, state, sl_id):
        print "[DEBUG] [SL]: %d -- State: %d" %(sl_id, state)

    def debugCont(self, cont, sl_id):
        print "[DEBUG] [SL]: %d -- Cont: %d" % (sl_id, cont)

    def debugMSG(self, text, sl_id):
        print "[DEBUG] [SL]: %d -- MSG: %s" % (sl_id, text)


    def printIteration(self, index):
        self.f = open('log.txt', 'a')
        text = "[LOG] - Iteration: " + str(index)
        print text
        self.f.write(text + "\n")
        self.f.close()

    def printmsg(self, id_serv, id_sl, id_db, msg, state):
        self.f = open('log.txt', 'a')
        text = "[SERVER " + str(id_serv) + "] [SL " + str(id_sl) + "] [DB " + str(id_db) + "] --- " + str(
            msg) + " ===> [" + state + "]"
        print text
        self.f.write(text + "\n")
        self.f.close()



def send():
    host = init_host()
    wd = os.path.dirname(os.path.realpath(__file__))
    streetLights = sum(1 for line in open(wd + '/input.txt'))

    servers = 4
    bds = 5

    # We share the SL between the servers
    if streetLights % bds == 0:
        sl_division = (streetLights / bds)
    else:
        sl_division = (streetLights / bds) + 1

    # create actors
    st = []  # StreetLight array
    serv = []  # Server array
    bd = []  # DataBase array

    #  create log
    log = host.spawn_id('1', 'P2', 'Log', [])

    #  create bds
    for i in range(bds):
        index = sl_division * i
        bd.append(host.spawn_id(str(i), 'P2', 'Bd', [sl_division, index, streetLights]))

    # create streetLights
    for i in range(streetLights):
        st.append(host.spawn_id(str(i), 'P2', 'StreetLight', [i % bds]))

    # create server
    for i in range(servers):
        serv.append(host.spawn_id(str(i), 'P2', 'Server', [st, log, i, bd]))

    # create queue
    queue = host.spawn_id('1', 'P2', 'Queue', [serv, servers])

    # Sensor.start('signal.txt')
    s = host.spawn_id('1', 'P2', 'Sensor', [st, log])

    for i in range(streetLights):
        st[i].set_queue(queue, i)

    s.start(wd + '/input.txt')

    print '=====[SERVER START]====='


if __name__ == '__main__':
    start_controller('pyactive_thread')
    serve_forever(send)
