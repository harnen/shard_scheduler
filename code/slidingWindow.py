import abc
import random
import networkx as nx
from policy import Policy
from collections import deque
from itertools import combinations
import random

class SlidingWindowPolicy(Policy):

    def __init__(self, num_shards, intra_shard_cost, inter_shard_cost, window_length, debug=False, seed = 0, load_only = False, migrate_contracts = False):
        self.num_shards = num_shards
        self.alignments = {}  # connectivity of accounts/contracts to shards
        self.shard_load = [1]*self.num_shards
        self.node_to_shard = {}
        self.past_txs = None
        # number of migrations
        self.migrations = 0
        self.intra_shard_cost = intra_shard_cost 
        self.inter_shard_cost = inter_shard_cost
        self.debug = debug
        self.inactive_nodes = {}
        self.window_length = window_length
        random.seed(seed)
        self.load_only = load_only
        self.migrate_contracts = migrate_contracts

    def analyzeMempoll(self, transaction_buffer):
        if self.past_txs is None:
            self.past_txs = deque([], maxlen=int(self.window_length))

        return transaction_buffer

    def add_tx_to_history(self, tx):
        removed = None
        if len(self.past_txs) == self.past_txs.maxlen:
            removed = self.past_txs.popleft()
            self.apply_removed(removed)
        
        self.past_txs.append(tx)

        return removed

    def apply_removed(self, tx_dict):
    
        for tx in tx_dict['load']:
            shard = tx[0]
            cost = tx[1]
            self.shard_load[shard] -= cost

        for tx in tx_dict['account']:
            account = tx[0]
            shard = tx[1]
            self.reduce_alignment(account, shard, 1)
    
    def get_least_loaded_shard(self):
        shard = self.shard_load.index(min(self.shard_load))

        return shard

    def getShardNum(self, txhash):
        
        if txhash not in self.node_to_shard:
            return None
        
        return self.node_to_shard[txhash]

    def reduce_alignment(self, account, shard, weight):
        if self.debug and account not in self.alignments:
            print('account: ', account, ' shard: ', shard)
        shard_weights = self.alignments[account]
        shard_weights[shard] -= weight
        if shard_weights[shard] < 0:
            print('Error: gravity for account ', account, ' is ', shard_weights[shard])

    def add_to_alignment(self, account, shard, weight):

        shard_weights = None
        if account in self.alignments:
            shard_weights = self.alignments[account]
            shard_weights[shard] += weight
        else:
            shard_weights = [1.0]*self.num_shards
            shard_weights[shard] += weight
            self.alignments[account] = shard_weights
                
    def get_alignment_with_shard(self, account, shard):

        shard_weights = None
        if account in self.alignments:
            shard_weights = self.alignments[account]
            return shard_weights[shard]
        else:
            return None

    def get_alignment_vector(self, account):
        
        if account in self.alignments:
            return self.alignments[account]
        else:
            return [1.0]*self.num_shards
    
    def should_migrate(self, account, dest_shard):
        alignment = self.get_alignment_vector(account)

        total_alignment = sum(alignment)

        # if more than half of the account's interactions are with dest_shard, then migrate
        if total_alignment < 2*alignment[dest_shard]:
            return True 

        return False

    def apply_tx_reactive(self, accounts_set, migrations, initial_allocations):

        tx = {}
        tx['account'] = []
        tx['load'] = []
        
        # Apply the initial allocations:
        for account, shard in initial_allocations.items():
            self.node_to_shard[account] = shard
        
        # Apply the migrations:
        for m in migrations:
            account = m[0]
            from_shard = m[1]
            to_shard = m[2]
            self.shard_load[from_shard] += self.inter_shard_cost
            self.shard_load[to_shard] += self.inter_shard_cost
            self.migrations += 1
            self.node_to_shard[account] = to_shard
            # self.add_to_alignment(account, to_shard, 1)
            # History data
            tx['load'].append((from_shard, self.inter_shard_cost))
            tx['load'].append((to_shard, self.inter_shard_cost))
            #tx['account'].append((account, to_shard))
            
        
        shards_set = set()
        shard_counts = {}
        for account in accounts_set:
            shard = self.node_to_shard[account] 
            shards_set.add(shard)
            if shard not in shard_counts.keys():
                shard_counts[shard] = 1
            else:
                shard_counts[shard] += 1
        
        for in_shard, out_shard in list(combinations(shards_set,2)):
            self.shard_load[in_shard] += self.inter_shard_cost
            self.shard_load[out_shard] += self.inter_shard_cost
            tx['load'].append((in_shard, self.inter_shard_cost))
            tx['load'].append((out_shard, self.inter_shard_cost))

        for from_n, to_n in list(combinations(accounts_set, 2)):
            from_shard = self.getShardNum(from_n) 
            to_shard = self.getShardNum(to_n)

            assert (from_shard is not None)
            assert (to_shard is not None)
            
            self.add_to_alignment(from_n, to_shard, 1.0)
            self.add_to_alignment(to_n, from_shard, 1.0)

            # Add tx history
            tx['account'].append((from_n, to_shard))
            tx['account'].append((to_n, from_shard))

        for shard in shard_counts.keys():        
            if shard_counts[shard] > 1:
                self.shard_load[shard] += self.intra_shard_cost 
                tx['load'].append((shard, self.intra_shard_cost))

        self.add_tx_to_history(tx)

    def updatePolicy_reactive(self, accounts_set, is_contract):

        unallocated_nodes_set = set()
        shard_choices = set()
        
        # if there are contracts that are already placed in a shard, their shards 
        # are the only choices for migrating others (we don't migrate contracts).
        for account in accounts_set:
            if is_contract[account] and account in self.node_to_shard.keys():
                shard_choices.add(self.node_to_shard[account])
            if(account not in self.node_to_shard.keys()):
                unallocated_nodes_set.add(account)

        if len(shard_choices) == 0 and len(unallocated_nodes_set) == 0:
            for account in accounts_set:
                if account in self.node_to_shard.keys():
                    shard = self.node_to_shard[account]
                    shard_choices.add(shard)

        # if there are unallocated accounts, we can consider all the shards as choices
        #elif len(shard_choices) == 0 and len(unallocated_nodes_set) > 0:
        if len(unallocated_nodes_set) > 0:
            for shard in range(self.num_shards):
                shard_choices.add(shard)

        if self.debug:
            print('Shard choices: ', shard_choices)

        aggregate_alignment = [0]*self.num_shards
        
        if self.load_only is True:
            aggregate_alignment = [1.0/self.shard_load[x] for x in range(self.num_shards)]

        else:
            for account in accounts_set:
                if self.debug:
                    print('Account alignment: ', self.get_alignment_vector(account))
                aggregate_alignment = [sum(x) for x in zip(aggregate_alignment, self.get_alignment_vector(account))]
            if self.debug:
                print('Final aggregate: ', aggregate_alignment)
            aggregate_alignment = [1.0*aggregate_alignment[x]/self.shard_load[x] for x in range(self.num_shards)]
            
        for shard in range(self.num_shards):
            if shard not in shard_choices:
                aggregate_alignment[shard] = -1
        if self.debug:
            print('Aggregate alignment: ', aggregate_alignment)
        highest_alignment = max(aggregate_alignment)
        shards = [x for x in range(self.num_shards) if aggregate_alignment[x] == highest_alignment] 
        optimal_shard = random.choice(shards)
        if self.debug:
            print('Optimal shard: ', optimal_shard)
                    
        # first place the unallocated accounts in the optimal shard:
        initial_allocations = {}
        for account in unallocated_nodes_set:
            initial_allocations[account] = optimal_shard
            #self.node_to_shard[account] = optimal_shard
        # migrate the rest of the accounts
        migrations = []
        # XXX stop condition
        for account in accounts_set.difference(unallocated_nodes_set):
            if (is_contract[account]) and (self.migrate_contracts is False):
                continue
            if self.node_to_shard[account] != optimal_shard:
                if self.should_migrate(account, optimal_shard):
                    migrations.append((account, self.node_to_shard[account], optimal_shard))
                    if self.debug:
                        print('Migration: ', account, ' from: ', self.node_to_shard[account], ' to: ', optimal_shard)
        
        return migrations, initial_allocations
    
    def updatePolicy(self, row):
        return
    
    def printStats(self):
        return [self.migrations]
