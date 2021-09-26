import abc
import random
import networkx as nx
from collections import Counter
import csv
from misc import *
import operator
from collections import Counter
import community as louvain
import math
import sys






class Policy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def getShardNum(self, txhash) -> (int):
        pass
    @abc.abstractmethod
    def updatePolicy(self, row):
        pass
    @abc.abstractmethod
    def analyzeMempoll(self, mempoll):
        pass
    #@abc.abstractproperty
    #def summary():
    #    pass
    @abc.abstractmethod
    def printStats(self):
        pass


class TheoreticBestPolicy(Policy):
    def __init__(self, shard_num):
        self.counter = 0
        self.table = []
        for i in range(0, shard_num):
            self.table.append(i)

    #compare_policies will always ask about a shard of in and out. This policy always returns the same shard for both, and performs round-robin between shards
    def getShardNum(self, txhash):
        result = self.table[self.counter % len(self.table)]
        return result
    def updatePolicy(self, row):
        pass
    def analyzeMempoll(self, mempoll):
        return mempoll
    def printStats(self):
        pass
    def updatePolicy_reactive(self, accounts_set, is_contract):
        return [],{}
    def apply_tx_reactive(self, accounts_set, is_contract, initial_allocations):
        self.counter += 1
        return

class LouvainPolicy(Policy):
    def __init__(self, shard_num, input_file):
        G = nx.Graph()
        with open(input_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                from_n = row['from']
                if(row['to'] != ''):
                    to_n = row['to']
                else:
                    to_n = row['contractAddress']

                if(G.has_edge(from_n, to_n)):
                    G[from_n][to_n]['weight'] += 1
                else:
                    G.add_edge(from_n, to_n, weight=1)
        self.communities = calculateLouvain(G, shard_num)        

    def updatePolicy(self, row):
        pass

    def getShardNum(self, txhash):
        return self.communities[txhash]
    
    def analyzeMempoll(self, mempoll):
        return mempoll
    def printStats(self):
        pass
    

class LouvainFirstPolicy(Policy):
    def __init__(self, shard_num, input_file):
        G = nx.Graph()
        self.shard_num = shard_num
        with open(input_file, newline='') as csvfile:
            self.limit = (sum(1 for _ in csvfile))/2
            csvfile.seek(0, 0)
            reader = csv.DictReader(csvfile)
            counter = 0
            for row in reader:
                from_n = row['from']
                if(row['to'] != ''):
                    to_n = row['to']
                else:
                    to_n = row['contractAddress']

                if(G.has_edge(from_n, to_n)):
                    G[from_n][to_n]['weight'] += 1
                else:
                    G.add_edge(from_n, to_n, weight=1)
                counter += 1
                if (counter > self.limit):
                    break
        self.mapping = calculateLouvain(G, shard_num)
        self.counter = 0

    def updatePolicy(self, row):
        #don't do anything during the first half
        #after that do as SameAsFirst  
        if(self.counter <= self.limit):
            self.counter += 1
            return
        from_n = row['from']
        to_n = ''
        if(row['to'] != ''):
            to_n = row['to']
        else:
            to_n = row['contractAddress']

        if(from_n in self.mapping and to_n not in self.mapping):
            self.mapping[to_n] = self.mapping[from_n]
        elif(from_n not in self.mapping and to_n in self.mapping):
            self.mapping[from_n] = self.mapping[to_n]
        elif(from_n not in self.mapping and to_n not in self.mapping):
            num = random.randint(0, self.shard_num-1)
            self.mapping[from_n] = num 
            self.mapping[to_n] = num 


    def getShardNum(self, txhash):
        return self.mapping[txhash]

    def analyzeMempoll(self, mempoll):
        return mempoll
    def printStats(self):
        pass


class SameAsFirstPolicy(Policy):

    def __init__(self, shards_num):
        self.mapping = {}
        self.users_in_shard = [0] * shards_num
        self.shard_num = shards_num
    
    def getShardNum(self, txhash):
        #translate keys into ints for better memory usage
        return self.mapping[txhash]

    def getBestShard(self):
        return random.randint(0, self.shard_num-1)

    def updatePolicy(self, row):
        from_n = row['from']
        to_n = ''
        if(row['to'] != ''):
            to_n = row['to']
        else:
            to_n = row['contractAddress']

        if(from_n in self.mapping and to_n not in self.mapping):
            self.mapping[to_n] = self.mapping[from_n]
            self.users_in_shard[self.mapping[to_n]] += 1
        elif(from_n not in self.mapping and to_n in self.mapping):
            self.mapping[from_n] = self.mapping[to_n]
            self.users_in_shard[self.mapping[from_n]] += 1
        elif(from_n not in self.mapping and to_n not in self.mapping):
            num = self.getBestShard()
            self.mapping[from_n] = num 
            self.mapping[to_n] = num 
            self.users_in_shard[num] += 2
    
    def analyzeMempoll(self, mempoll):
        return mempoll
    def printStats(self):
        pass


class HashPolicy(Policy):

    def __init__(self, shards_num):        
        self.shards_num = shards_num
        self.min_addr = '0x0000000000000000000000000000000000000000'
        self.max_addr = '0x10000000000000000000000000000000000000000'
        self.step = int(int(self.max_addr, 16) / shards_num)

    def updatePolicy(self, mempoll):
        pass

    def getShardNum(self, txhash):
        #return int(int(txhash, 16) / self.step)
        return int(txhash, 16) % self.shards_num

    def analyzeMempoll(self, mempoll):
        return mempoll
    def printStats(self):
        pass

    def updatePolicy_reactive(self, accounts_set, is_contract):
        return [],{}

    def apply_tx_reactive(self, accounts_set, migrations, initial_allocations):
        return

# tx = 'ab75c0e0f4c6afc8fbb11a3935304a164d4a0f37454c8568b02401b4428ecc8e'
#tx = '0x9e3d69305Da51f34eE29BfB52721e3A824d59e69'
#tx = '0xFFfFfFffFFfffFFfFFfFFFFFffFFFffffFfFFFfF'
#policy = RandomPolicy(10)
#num = policy.getShardNum(tx)

#print("Tx", tx, "in shard", num)

class DynamicPolicy(Policy):
    def __init__(self, shardNum, capacity, variant = 1, d = 0.5, m = 1, intra_cost = 1, inter_cost = 1.5):
        self.mapping = {}
        self.G = nx.Graph()
        self.shardNum = shardNum
        self.counter = 0
        self.shardLoad = [0] * shardNum
        self.capacity = capacity
        self.partition = {}
        self.variant = variant
        self.gravity = {}
        self.same = 0
        self.diff = 0
        self.Nsame = 0
        self.Ndiff = 0
        self.counter = 0
        self.limit = 100
        self.d = d
        self.m = m
        self.num_migrations = 0
        self.intra_cost = intra_cost
        self.inter_cost = inter_cost


    def getShardNum(self, node):
        if node in self.mapping:
            return self.mapping[node]


    def mostConnected(self, node):
        connections = {}
        for n in self.G.neighbors(node):
            shard = self.getShardNum(n)
            weight = self.G[node][n]['weight']
            if(shard not in connections):
                connections[shard] = weight
            else:
                connections[shard] += weight
        mostConnected = max(connections, key=connections.get)

        if(self.getShardNum(node) in connections):
            diff = connections[self.getShardNum(node)] - connections[mostConnected]
        else:
            diff = connections[mostConnected]
        return  mostConnected, diff


    def migrate(self, migrations):
        #print("will do", len(migrations), "migrations")
        self.num_migrations += len(migrations)
        for node in migrations:
            assert(migrations[node]['from'] == self.mapping[node])
            self.mapping[node] = migrations[node]['to']
            #print(node, migrations[node]['from'], "-->", migrations[node]['to'])

    def updatePolicy(self, row):
        from_n = row['from']
        to_n = ''
        if(row['to'] != ''):
            to_n = row['to']
        else:
            to_n = row['contractAddress']

        if(self.G.has_edge(from_n, to_n)):
            self.G[from_n][to_n]['weight_base'] += 1
        else:
            self.G.add_edge(from_n, to_n, weight_base=1)

        if(from_n not in self.partition):
            self.partition[from_n] = len(self.partition)

        if(to_n not in self.partition):
            self.partition[to_n] = len(self.partition)
    
    def getMultiplier(self, shard):
        #print("shardLoad now", self.shardLoad)
        total = sum(self.shardLoad)
        fraction = self.shardLoad[shard]/total
        if(fraction == 0):
            fraction = 1
        multiplier = fraction * self.shardNum
        #return 1/multiplier
        return math.pow(1/multiplier, self.m)



    def calculateGraphWeights(self):
        G = self.G.copy()
        for edge in G.edges(data=True):
            multiplier1 = self.getMultiplier(self.mapping[edge[0]])
            multiplier2 = self.getMultiplier(self.mapping[edge[1]])
            multiplier = max([multiplier1, multiplier2])
            #print("s1", self.mapping[edge[0]], "s2", self.mapping[edge[1]])
            #print("multiplier for edge", edge, "=", multiplier)
            self.G[edge[0]][edge[1]]['weight'] = multiplier * edge[2]['weight_base']
            #edge[2]['weight'] = multiplier * edge[2]['weight_base']
            if(self.G[edge[0]][edge[1]]['weight'] == 0):
                self.G.remove_edge(*edge[:2])


    def calculateLoad(self, mempoll):
        activeNodes = {}
        migrations = {}
        intraTX = []
        interTX = []
        #divide historical data by 2
        self.shardLoad = [x / 2 for x in self.shardLoad]

        for tx in mempoll:
            from_n = tx['from']
            if(tx['to'] != ''):
                to_n = tx['to']
            else:
                to_n = tx['contractAddress']
            assert(operator.xor(tx['to'] != '', tx['contractAddress'] != ''))
            nodes = [from_n, to_n]


            for node in nodes:
                if(node not in activeNodes):
                    activeNodes[node] = {}
                    activeNodes[node]['transfers'] = 1
                else:
                    activeNodes[node]['transfers'] += 1
                if(node not in self.mapping):
                    num = self.shardLoad.index(min(self.shardLoad))
                    self.mapping[node] = num
                if(node not in self.G.nodes()):
                    #print("Bringing", node, "back.")
                    self.G.add_node(node)


            in_shard = self.getShardNum(from_n)
            out_shard = self.getShardNum(to_n)
            #print(from_n, "(", in_shard, ")-->", to_n, "(", out_shard,")")
            if(in_shard == out_shard):
                self.shardLoad[in_shard] += self.intra_cost
                intraTX.append(tx)
            else:
                self.shardLoad[in_shard] += self.inter_cost
                self.shardLoad[out_shard] += self.inter_cost
                interTX.append(tx)
                #we want everything to be symmetric
                #a possible migration from A to B is
                #also a possible migration from B to A
                targets = [[from_n, out_shard], [to_n, in_shard]]
                for move in targets:     
                    node = move[0]
                    targetShard = move[1]
                    
                    if(node not in migrations):
                        migrations[node] = {}
                    if(targetShard not in migrations[node]):
                        migrations[node][targetShard] = 1
                    else:
                        migrations[node][targetShard] += 1
                
            
            #print(shardLoad)
        #print("sum(shardLoad):", sum(shardLoad), "max:", (self.inter_cost * len(mempoll)*2))
        #assert(sum(self.shardLoad) <= (self.inter_cost * len(mempoll)*2)) no longer used as we eep historical data
        #print("activeNodes", activeNodes)
        #print("shardLoad", shardLoad)
        #print("migrations", migrations)
        return activeNodes, migrations, intraTX, interTX


    def calculateGravity(self):
        self.gravity = {}
        for node in self.partition:
            community = self.partition[node]
            shard = self.mapping[node]
            #intra-community edges - inter-community edges
            nodeWeight = 0
            for edge in self.G.edges(node, data=True):
                #edge is in format (<node>, <peer>, {'weight': <weight>})
                peer = edge[1]
                weight = edge[2]['weight']
                if(self.partition[node] == self.partition[peer]):
                    nodeWeight += weight
                else:
                    nodeWeight -= weight
            if community not in self.gravity:
                self.gravity[community] = {}
            if shard not in self.gravity[community]:
                self.gravity[community][shard] = 0
            #maybe we should only add positive values here?
            self.gravity[community][shard] += nodeWeight

        return self.gravity
            

                


    def currentConnected(self, mempoll):
        shardG = nx.DiGraph()
        for tx in mempoll:
            from_n = tx['from']
            if(tx['to'] != ''):
                to_n = tx['to']
            else:
                to_n = tx['contractAddress']
            assert(operator.xor(tx['to'] != '', tx['contractAddress'] != ''))

            shardIn = self.getShardNum(to_n)
            shardOut = self.getShardNum(from_n)
            if(shardIn != shardOut):
                if(shardG.has_edge(shardIn, shardOut)):
                    shardG[shardIn][shardOut]['weight'] += 1
                    
                else:
                    shardG.add_edge(shardIn, shardOut, weight=1, nodes=set())
                
                shardG[shardIn][shardOut]['nodes'].add(from_n)
        
        return shardG

    def cleanGraph(self):
        G = self.G.copy()
        #print("Nodes before cleaning", G.nodes)
        #print("Edges before cleaning", G.edges(data=True))
        for edge in G.edges(data=True):
            self.G[edge[0]][edge[1]]['weight_base'] -= self.d
            #edge[2]['weight'] = multiplier * edge[2]['weight_base']
            if(self.G[edge[0]][edge[1]]['weight_base'] <= 0):
                #print("Removing edge", edge, file=sys.stderr)
                self.G.remove_edge(*edge[:2])
            if( len(self.G.edges(edge[0]) ) == 0):
                #print("Removing node", edge[0], file=sys.stderr)
                self.G.remove_node(edge[0])
            if( len(self.G.edges(edge[1]) ) == 0):
                #print("Removing node", edge[1], file=sys.stderr)
                    #when users sends transactions to themselves
                    #they'll have an edge to themselves so both nodes will be the same
                if(edge[1] in self.G.nodes()):
                    self.G.remove_node(edge[1])


    def analyzeMempoll(self, mempoll):
        #print("Analyze mempoll")
        #print("Analyze mempoll", file=sys.stderr)
        self.counter += 1
        #We could maybe just half the previous values?
        activeNodes, moves, intraTX, interTX = self.calculateLoad(mempoll)
        self.calculateGraphWeights()
        #print("~~~~~~~~EDGES~~~~~~~~~")
        #for edge in self.G.edges(data=True):
        #    print(edge)
        #print("~~~~~~~~~~~~~~~~~~~~~~")
        self.partition = louvain.best_partition(self.G)#, partition=self.partition)
        gravity = self.calculateGravity()
        #print("Shard load before:", self.shardLoad)  
        #print("Nodes in shards", Counter(self.mapping.values()))
        #print("Gravity", gravity)
        #print("Moves", moves)
        #print("Mapping", self.mapping)
        #print("Communities", self.partition)
        #print("Communities size:", calculateGroupSizes(self.partition, self.G))
        #print("Migrations", moves)  
        migrations = {}

        if(self.variant == 2):
            #print("n1, s1, c1 <--> n2, s2, c2")
            for tx in interTX:
                node1 = tx['from']
                if(tx['to'] != ''):
                    node2 = tx['to']
                else:
                    node2 = tx['contractAddress']
                shard1 = self.mapping[node1]
                shard2 = self.mapping[node2]
                community1 = self.partition[node1]
                community2 = self.partition[node2]
                load1 = self.shardLoad[shard1]
                load2 = self.shardLoad[shard2]
                gravity1 = gravity[community1][shard1]
                gravity2 = gravity[community2][shard2]
                txNum1 = moves[node1][shard2]
                txNum2 = moves[node2][shard1]
                #print(node1, shard1, community1, "<-->",  node2, shard2, community2)
                #when to move?
                node = None
                if(community1 == community2):
                    self.same += 1
                    if(gravity1 > gravity2):
                        #print("moving s2 -> s1 (G)")
                        node = node2
                        currentShard = shard2
                        targetShard = shard1
                        txNum = txNum2
                    elif(gravity1 < gravity2):
                        #print("moving s1 -> s2 (G)")
                        node = node1
                        currentShard = shard1
                        targetShard = shard2
                        txNum = txNum1
                    elif(load1 > load2):
                        #print("moving s1 -> s2 (L)")
                        node = node1
                        currentShard = shard1
                        targetShard = shard2
                        txNum = txNum1
                    elif(load2 > load1):
                        #print("moving s2 -> s1 (L)")
                        node = node2
                        currentShard = shard2
                        targetShard = shard1
                        txNum = txNum2
                    else:
                        self.Nsame += 1
                else:
                    self.diff += 1
                    if(shard1 not in gravity[community2]):
                        gravity[community2][shard1] = 0
                    if(shard2 not in gravity[community1]):
                        gravity[community1][shard2] = 0

                    if( gravity1 <= gravity[community1][shard2]
                    ):
                        node = node1
                        currentShard = shard1
                        targetShard = shard2
                        txNum = txNum1
                    elif( gravity2 <= gravity[community2][shard1]
                    ):
                        node = node2
                        currentShard = shard2
                        targetShard = shard1
                        txNum = txNum2
                    else:
                        self.Ndiff += 1


                if( (node != None) and 
                (node not in migrations)):
                    migrations[node] = {}
                    migrations[node]['from'] = currentShard
                    migrations[node]['to'] = targetShard
                    self.shardLoad[targetShard] += txNum
                    self.shardLoad[currentShard] -= self.inter_cost * (txNum - 1)
    
        else:
            for node in moves:
                currentShard = self.mapping[node]
                community = self.partition[node]
                #sort the potential migrations (to shard X) by number of transactions we have with this shard
                sortedMoves = {k: v for k, v in sorted(moves[node].items(), key=lambda item: item[1], reverse=True)}

                for targetShard in sortedMoves:
                    transactionNum = sortedMoves[targetShard]
                    currentShardGrav = gravity[community][currentShard]
                    if(targetShard in gravity[community]):
                        targetShardGrav = gravity[community][targetShard]
                    else:
                        targetShardGrav = 0
                    #print("node:", node, "currentShard:", currentShard, "targetShard:", targetShard, "community", community)
                    #print("transactionNum:", transactionNum, "currentShardGrav:", currentShardGrav, "targetShardGrav", targetShardGrav)
                    #print("shardLoad[currentShard]:", shardLoad[currentShard], "shardLoad[targetShard]:", shardLoad[targetShard], "self.capacity:", self.capacity)
                    #print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
                    avgLoad = sum(self.shardLoad)/self.shardNum
                    if((self.variant == 1) and
                        #((transactionNum > 1) or
                        ((currentShardGrav <= targetShardGrav) and
                        ((self.shardLoad[currentShard] > avgLoad) and (self.shardLoad[targetShard] < avgLoad)))
                    ):
                        migrations[node] = {}
                        migrations[node]['from'] = currentShard
                        migrations[node]['to'] = targetShard
                        self.shardLoad[targetShard] += transactionNum
                        self.shardLoad[currentShard] -= self.inter_cost * (transactionNum - 1)
                        #don't move the same node anymore
                        break

        self.migrate(migrations)
        #activeNodes, shardLoad, migrations = self.calculateLoad(mempoll)
        #print("Shard load after:", self.shardLoad)
        #input('--> ')
        #inputif(self.counter % self.limit == 0):
        #print("Clean graph", file=sys.stderr)
        self.cleanGraph()
        #print("After cleaning graph", file=sys.stderr)
        return intraTX + interTX
        
        # self.counter += 1
        # print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        # if(self.counter == 100 ):
        #     quit()
    def printStats(self):
        print("DynamicPolicy v", self.variant)
        print("self.same", self.same,"(", self.Nsame, ") self.diff", self.diff, "(", self.Ndiff, ")")
        return [self.num_migrations, self.variant, self.d, self.m]
    
    def updatePolicy_reactive(self, from_n, to_n):
        return [],{}
    def apply_tx_reactive(self, from_n, to_n, migrations, initial_allocations):
        return
