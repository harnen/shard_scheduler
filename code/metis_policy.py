import abc
import random
import networkx as nx
from collections import Counter
import csv
#from misc import *
import metis
from policy import Policy
#import nxmetis

class MetisPolicy(Policy):
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
                
                if 'num_txs' in G.nodes[from_n]:
                    G.nodes[from_n]['num_txs'] += 1
                else:
                    G.nodes[from_n]['num_txs'] = 1

                if 'num_txs' in G.nodes[to_n]:
                    G.nodes[to_n]['num_txs'] += 1
                else:
                    G.nodes[to_n]['num_txs'] = 1
        
        G.graph['node_weight_attr'] = 'num_txs'
        G.graph['edge_weight_attr'] = 'weight'
        
        partition_weights = [1.0/shard_num] * shard_num
        edge_cuts, partitions = metis.part_graph(G, shard_num, partition_weights)
        self.communities = {}
        for index, node in enumerate(G.nodes()):
            self.communities[node] = partitions[index]
            if partitions[index] > shard_num:
                print('Error: node ', node, ' has a shard number of ', partitions[index])

        #self.communities = calculateLouvain(G, shard_num)        

    def updatePolicy(self, row):
        pass

    def getShardNum(self, txhash):
        return self.communities[txhash]
    
    def analyzeMempoll(self, mempoll):
        return mempoll
    def printStats(self):
        pass
    

class MetisFirstPolicy(Policy):
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
                
                if 'num_txs' in G.nodes[from_n]:
                    G.nodes[from_n]['num_txs'] += 1
                else:
                    G.nodes[from_n]['num_txs'] = 1

                if 'num_txs' in G.nodes[to_n]:
                    G.nodes[to_n]['num_txs'] += 1
                else:
                    G.nodes[to_n]['num_txs'] = 1

                counter += 1
                if (counter > self.limit):
                    break
        
        G.graph['node_weight_attr'] = 'num_txs'
        G.graph['edge_weight_attr'] = 'weight'
        
        partition_weights = [1.0/shard_num] * shard_num
        edge_cuts, partitions = metis.part_graph(G, shard_num, partition_weights)
        self.mapping = {}
        for index, node in enumerate(G.nodes()):
            self.mapping[node] = partitions[index]
            if partitions[index] > shard_num:
                print('Error: node ', node, ' has a shard number of ', partitions[index])
        #self.mapping = calculateLouvain(G, shard_num)
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


