import abc
import random
import networkx as nx
from policy import Policy
from collections import deque
from itertools import combinations

class MichalPolicy(Policy):

    def __init__(self, num_shards, intra_shard_cost, inter_shard_cost, use_allignment = False, global_optimal=False, debug=False, migrate_contracts = False):
        self.num_shards = num_shards
        self.V = {}
        # add centroid nodes
        
        self.shard_load = [0]*self.num_shards
        self.node_to_shard = {}
        self.past_txs = None
        # number of migrations
        self.migrations = 0
        self.intra_shard_cost = intra_shard_cost 
        self.inter_shard_cost = inter_shard_cost
        self.debug = debug
        self.global_optimal = global_optimal
        self.use_allignment = use_allignment
        self.migrate_contracts = migrate_contracts

    def analyzeMempoll(self, transaction_buffer):
        self.window_length = len(transaction_buffer) 
        if self.past_txs is None:
            self.past_txs = deque([], maxlen=self.window_length)

        return transaction_buffer

    def add_tx_to_history(self, tx):
        removed = None
        if len(self.past_txs) == self.past_txs.maxlen:
            removed = self.past_txs.popleft()
            self.apply_removed(removed)
        
        self.past_txs.append(tx)

        return removed

    def apply_removed(self, tx_list):
        for tx in tx_list:
            account = tx[0]
            shard = tx[1]
            cost = tx[2]
            
            #if account == '0x004B37d3e250EB1826Fc6a587DEDdc18E7bE6F6f':
            #    print('Deducting from account: ', account, ' shard: ', shard, ' gravity: ', self.G[account][self.shard_nodes[shard]]['weight']) 

            if account is not None:
                gravity = self.G[account][self.shard_nodes[shard]]['weight']
                if gravity <= 0:
                    print('Error: gravity for account ', account, ' is ', gravity)
                self.G[account][self.shard_nodes[shard]]['weight'] -= 1

            load = self.shard_load[shard]
            if load <= cost:
                print('Error: load on shard', shard, ' is: ', load, ' and it it less than ', cost)
            self.shard_load[shard] -= cost

            # Remove the node from the graph if all its edges have 0 weight
            # add the removed node to inactive_nodes dict
            if account is not None:
                current_shard = self.G.nodes[account]['shard']
                if self.G[account][self.shard_nodes[shard]]['weight'] == 0:
                    #self.G.remove_edge(account, self.shard_nodes[shard])
                    total_weight = 0
                    for nghbr in self.G[account]:
                        total_weight += self.G[account][nghbr]['weight']
                    if total_weight == 0:
                        self.G.remove_node(account)
                        self.inactive_nodes[account] = current_shard
                        if self.debug:
                            print('Account: ', account, ' is removed')
                        #if account == '0x004B37d3e250EB1826Fc6a587DEDdc18E7bE6F6f':
                        #    print('Account: ', account, ' is removed!')
    
    def get_least_loaded_shard(self, shards_set = None):

        if shards_set is None:
            shard = self.shard_load.index(min(self.shard_load))

            return shard

        else:
            min_load_shard = None
            min_load = float('inf')
            for shard in shards_set:
                load = self.shard_load[shard]
                if load < min_load:
                    min_load = load
                    min_load_shard = shard

            return min_load_shard
    
    def should_migrate(self, account, dest_shard):
        #return True
        src_shard = self.node_to_shard[account]
        if((self.V[account][src_shard] > 50) and (self.V[account][src_shard] >= sum(self.V[account]))):
            #print("Check_migratibility of ", account, 'from',  src_shard, 'to',  dest_shard)
            #print("It's V", self.V[account])
            #print("Return False")
            #quit()
            return False
        #print("return True")
        return True

    def getShardNum(self, txhash):
        if txhash not in self.node_to_shard:
            print('Error node: ', txhash, ' is not in mapping')
        
        return self.node_to_shard[txhash]
    
    #take into account load difference (how much, not only if there's)
    #take into account connections with uninvolved shards
    def apply_tx_reactive(self, accounts_set, migrations, initial_allocations):

        # Apply the initial allocations:
        for account, shard in initial_allocations.items():
            self.node_to_shard[account] = shard

        # Apply the migrations:
        if len(migrations) > 0:
            for m in migrations:
                account = m[0]
                from_shard = m[1]
                to_shard = m[2]
                self.shard_load[from_shard] += self.inter_shard_cost
                self.shard_load[to_shard] += self.inter_shard_cost
                self.node_to_shard[account] = to_shard
                self.migrations += 1

        shards_set = set()
        shard_counts = {}
        for account in accounts_set:
            shard = self.node_to_shard[account]
            self.V[account][shard] += 1
            shards_set.add(shard)
            if shard not in shard_counts.keys():
                shard_counts[shard] = 1
            else:
                shard_counts[shard] += 1
        
        for in_shard, out_shard in list(combinations(shards_set, 2)):
            self.shard_load[in_shard] += self.inter_shard_cost
            self.shard_load[out_shard] += self.inter_shard_cost 
        
        for from_n, to_n in list(combinations(accounts_set, 2)):
            from_shard = self.node_to_shard[from_n]
            to_shard = self.node_to_shard[to_n]
       
            self.V[from_n][to_shard] += 1
            self.V[to_n][from_shard] += 1
            
        for shard in shard_counts.keys():        
            if shard_counts[shard] > 1:
                self.shard_load[shard] += self.intra_shard_cost 

    def updatePolicy_reactive(self, accounts_set, is_contract):
        unallocated_nodes_set = set()
        shard_choices = set()
        global_optimal = self.global_optimal
        
        is_any_contract = any(is_contract.values())
        if is_any_contract:
            # if there are contracts that are already placed in a shard, their shards 
            # are the only choices for migrating others (we don't migrate contracts).
            for account in accounts_set:
                if is_contract[account] and account in self.node_to_shard.keys():
                    shard_choices.add(self.node_to_shard[account])
        
        if is_any_contract and len(shard_choices) > 0:
            # if there are unmovable contracts, then selection of any (global optimal) shard is not possible.
            global_optimal = False

        if len(shard_choices) == 0 and global_optimal is False:
            # if there is no restriction due to "unmigratable" contract accounts and local optimal selection. 
            for account in accounts_set:
                if account in self.node_to_shard.keys():
                    shard = self.node_to_shard[account]
                    shard_choices.add(shard)

        for account in accounts_set:
            if account not in self.node_to_shard.keys():
                unallocated_nodes_set.add(account)      
                self.V[account] = [0]*self.num_shards 
        
        if len(unallocated_nodes_set) > 0:
            for x in range(self.num_shards):
                shard_choices.add(x) 

        # optimal shard is the least-loaded shard either globally or from the given choices
        optimal_shard = None
        if len(shard_choices) > 0:
            optimal_shard = self.get_least_loaded_shard(shard_choices)
        else:
            # obtain globally optimal if no choices are given
            optimal_shard = self.get_least_loaded_shard()

        # first place the unallocated accounts in the optimal shard:
        initial_allocations = {}
        for account in unallocated_nodes_set:
            initial_allocations[account] = optimal_shard
            #self.node_to_shard[account] = optimal_shard
        # migrate the rest of the accounts
        migrations = []
        for account in accounts_set.difference(unallocated_nodes_set):
            if (is_contract[account]) and (self.migrate_contracts is False):
                continue
            if self.node_to_shard[account] != optimal_shard:
                if(self.use_allignment == True):
                    if(self.should_migrate(account, optimal_shard) == True):
                        migrations.append((account, self.node_to_shard[account], optimal_shard))
                else:
                    migrations.append((account, self.node_to_shard[account], optimal_shard))

        return migrations, initial_allocations

    def updatePolicy(self, row):
        return
    
    def printStats(self):
        return [self.migrations]
