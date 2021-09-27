import pandas as pd
import numpy as np
import matplotlib
#matplotlib.use('Agg')#enables running matplotlib on servers without graphical environment
import matplotlib.pyplot as plt
import csv
import sys
import getopt
from policy import *
#from slidingWindow import SlidingWindowPolicy
from shardSchedulerPolicy import ShardSchedulerPolicy
from futurePolicy import FuturePolicy
from metis_policy import *
from itertools import combinations
from scipy.stats import pearsonr
from scipy.stats import spearmanr
import operator
import time
import json


# defaults
BUFFER_SIZE = 1
SHARDS_NUM = 10
SHARD_CAPACITY = 100
TIME_INTERVAL = 10

#INPUT_FILE = '/space/michal/blockchain_sharding/code/ethereum/input/data_small.csv'
#INPUT_FILE = './input/data_500k_lines.csv' 
#INPUT_FILE = './input/multi_small.csv'
INPUT_FILE = '/space/michal/blockchain_sharding/code/ethereum/input/data_0-8163516.csv'
#INPUT_FILE='input/simple_data_100lines.csv'
OUT_DIR = 'output/'

#required for large csv fields (such as transaction input)
csv.field_size_limit(sys.maxsize)



def evaluate_policy(input_file, buffer_ratio, shards_num, shard_capacity, time_interval, policy, inter_cost, chainspace_output=False):
    steps = 0
    transaction_buffer = []
    #stats
    total_load = []
    total_wasted = []
    total_inter = []
    total_intra = []
    total_latency = []

    iteration = 0
    
    shard_txs = {}
    rounD = -1
    num_migrations = 0
    contracts = set()
    buffer_size = shards_num * shard_capacity * buffer_ratio
    print('The real Buffer size is : ', buffer_size)
    
    with open(input_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        numberOfRowsRead = 0
        for row in reader:
            numberOfRowsRead += 1
            policy.updatePolicy(row)
            # Parse the row and produce a list of contracts
            row = dict(row)
            text = str(row).replace('\'', '"').replace('\"[', '[').replace(']\"', ']')
            obj = json.loads(text)
            if(row['contractAddress'] != ''):
                contracts.add(row['contractAddress'].lower())
            involved = set([row['contractAddress'].lower(), row['from'].lower(), row['to'].lower()] + obj['affected'])
            involved.discard(str(''))
            involved.discard(str('0x0000000000000000000000000000000000000000'))
            transaction_buffer.append((steps, involved))
            if(len(transaction_buffer) >= buffer_size):
                #allow the policy to reorder pending transactions
                transaction_buffer = policy.analyzeMempoll(transaction_buffer)    
                if( (steps % time_interval) == 0):
                    total_load.append([0] * shards_num)
                    total_wasted.append([0] * shards_num)
                    total_inter.append([0] * shards_num)
                    total_intra.append([0] * shards_num)
                    total_latency.append([0] * shards_num)
                
                index = steps // time_interval
                steps += 1
                #print('iteration: ', iteration)
                iteration += 1
                #print('Number of rows read: ', numberOfRowsRead)

                shard_capacities = [shard_capacity] * shards_num
                leftovers = []
                rounD += 1
                shard_txs[rounD] = []
                for mempool_tx in transaction_buffer: 
                    # A mempool_tx is a pair: step_no, set of acounts                   
                    accounts_set = mempool_tx[1]
                    is_contract = {account : account in contracts for account in accounts_set}
                    migrations, initial_allocations = policy.updatePolicy_reactive(accounts_set, is_contract)

                    # Check if the tx can be processed (there is enough capacity)
                    # Assumption: either a tx as a whole is processed all together or not
                    # in the current round.
                    shard_costs = {}
                    migrated_accounts = {}
                    
                    # Compute the tx cost for each shard due to migrations
                    for migration in migrations:
                        account = migration[0]
                        from_shard = migration[1]
                        to_shard = migration[2]
                        if account in contracts:
                            sys.exit('Attempting to migrate a contract')
                        migrated_accounts[account] = to_shard
                        for shard in [from_shard, to_shard]:
                            if shard in shard_costs.keys():
                                shard_costs[shard] += inter_cost
                            else:
                                shard_costs[shard] = inter_cost
                                    
                    # Compute the tx costs due to inter-shard         
                    shards_set = set()
                    shard_counts = {}
                    for account in accounts_set:
                        if account in migrated_accounts.keys():
                            shard = migrated_accounts[account]
                        else:
                            if account in initial_allocations.keys(): 
                                shard = initial_allocations[account]
                            else:
                                shard = policy.getShardNum(account)
                        shards_set.add(shard)
                        if shard not in shard_counts.keys():
                            shard_counts[shard] = 1
                        else:
                            shard_counts[shard] += 1
                    
                    for in_shard, out_shard in list(combinations(shards_set,2)):
                        for shard in [in_shard, out_shard]:
                            if shard in shard_costs.keys():
                                shard_costs[shard] += inter_cost
                            else:
                                shard_costs[shard] = inter_cost
                    
                    # Compute the tx costs due to intra-shard         
                    for shard in shard_counts.keys():        
                        if shard_counts[shard] > 1:
                            if shard not in shard_costs.keys():
                                shard_costs[shard] = 1
                            else:
                                shard_costs[shard] += 1

                    # Check if the total cost per-shard is above the remaining capacity
                    apply_tx = True
                    for shard in shard_costs:
                        if shard_capacities[shard] < shard_costs[shard]:
                            apply_tx = False
                            # tx can not be applied (not enough capacity)
                            break 

                    if apply_tx is True: 
                        # Apply the tx
                        inter_shard = False
                        #num_migrations += len(migrations)
                        if len(migrations) > 0:
                            num_migrations += 1
                        # Apply the migrations
                        for account in migrated_accounts.keys():
                            in_shard = policy.getShardNum(account)
                            out_shard = migrated_accounts[account]
                            for shard in [in_shard, out_shard]:
                                shard_capacities[shard] -= inter_cost
                                total_load[index][shard] += inter_cost
                                inter_shard = True
                                #total_inter[index][shard] += 1
                            if chainspace_output:
                                shard_txs[rounD].append(([in_shard, out_shard], [in_shard, out_shard]))
                        # Apply the inter-shard txs (except migrations)
                        for in_shard, out_shard in list(combinations(shards_set,2)):
                            for shard in [in_shard, out_shard]:
                                shard_capacities[shard] -= inter_cost
                                total_load[index][shard] += inter_cost
                                #total_inter[index][shard] += 1
                                inter_shard = True
                            if chainspace_output:
                                shard_txs[rounD].append(([in_shard, out_shard], [in_shard, out_shard]))
                        # Apply the intra-shard txs
                        for shard in shard_counts.keys():        
                            if shard_counts[shard] > 1:
                                shard_capacities[shard] -= 1
                                total_load[index][shard] += 1
                                total_intra[index][shard] += 1
                                if chainspace_output:
                                    shard_txs[rounD].append(([shard], [shard]))
                        policy.apply_tx_reactive(accounts_set, migrations, initial_allocations)
                        total_latency[index][shard] += steps - mempool_tx[0]
                        if inter_shard:
                            total_inter[index][shard] += 1
                    else: # leave the tx to future rounds 
                        leftovers.append(mempool_tx)

                for i in range(0, shards_num): 
                    total_wasted[index][i] += shard_capacities[i]
                    #print('wasted capacity at shard: ', i, ' is ', shard_capacities[i])
                #print('Number of leftovers: ', len(leftovers))
                transaction_buffer = leftovers
    
    if ( (len(shard_txs) > 0) and chainspace_output ):
        for rounD in shard_txs.keys():
            output_file = open("chainspace_inputs/" + policy.__class__.__name__ + "_round_" + str(rounD) + ".txt", "w")
            for tx in shard_txs[rounD]:
                line = str(tx[0]) + ":" + str(tx[1]) + "\n"
                output_file.write(line)
            output_file.close()

    return {'steps': steps, 'wasted_capacity': total_wasted, 'inter': total_inter, 'intra': total_intra, 'load': total_load, 'num_migrations' : num_migrations, 'latency' : total_latency}

def parse_accounts(account_str):

    account_str = account_str[1:len(account_str)-1] # remove [ and  ]
    account_str = account_str.upper()
    if len(account_str) == 0:
        return []

    if ',' not in account_str: # single account in the list
        account = account_str[1:len(account_str)-1] # remove ' and '
        return [account]
    else: # multiple accounts in the list
        list_of_accounts = account_str.split(', ')  
        list_of_accounts = [x[1:len(x)-1] for x in list_of_accounts] # remove ' and '
        return list_of_accounts     

def make_patch_spines_invisible(ax):
    ax.set_frame_on(True)
    ax.patch.set_visible(False)
    for sp in ax.spines.values():
        sp.set_visible(False)

def plot_results(prefix, results):
    policies = results.keys()
    y_pos = np.arange(len(policies))

    width =0.1
    fig, host = plt.subplots(figsize=(15,6))
    fig.subplots_adjust(right=0.75)
    par1 = host.twinx()
    par2 = host.twinx()
    par2.spines["right"].set_position(("axes", 1.08))
    make_patch_spines_invisible(par2)
    par2.spines["right"].set_visible(True)
    par3 = host.twinx()
    par3.spines["right"].set_position(("axes", 1.16))
    make_patch_spines_invisible(par3)
    par3.spines["right"].set_visible(True)
    par4 = host.twinx()
    par4.spines["right"].set_position(("axes", 1.24))
    make_patch_spines_invisible(par4)
    par4.spines["right"].set_visible(True)
    par5 = host.twinx()
    par5.spines["right"].set_position(("axes", 1.32))
    make_patch_spines_invisible(par5)
    par5.spines["right"].set_visible(True)
    
    steps = [v['steps'] for (k, v) in results.items()]
    p1 = host.bar(y_pos, steps, align='center', width=width, label='steps')
    plt.xticks(y_pos, policies)

    wasted = [v['wasted_capacity'] for (k, v) in results.items()]
    p2 = par1.bar(y_pos + width, wasted, alpha=0.5, width=width, color='orange', label='wasted_capacity')

    inter = [v['inter'] for (k, v) in results.items()]
    p3 = par2.bar(y_pos + 2*width, inter, width=width, color='green', label='inter_transactions')

    intra = [v['intra'] for (k, v) in results.items()]
    p4 = par3.bar(y_pos + 3*width, intra, width=width, color='red', label='intra_transactions')

    load = [v['load'] for (k, v) in results.items()]
    p5 = par4.bar(y_pos + 4*width, load, width=width, color='yellow', label='load')

    movement = [v['num_migrations'] for (k,v) in results.items()]
    p6 = par5.bar(y_pos + 5*width, movement, width=width, color='black', label='migrations')

    tkw = dict(size=5, width=1.5)
    host.tick_params(axis='y', colors='blue', **tkw)
    par1.tick_params(axis='y', colors='orange', **tkw)
    par2.tick_params(axis='y', colors='green', **tkw)
    par3.tick_params(axis='y', colors='red', **tkw)
    par4.tick_params(axis='y', colors='yellow', **tkw)
    par5.tick_params(axis='y', colors='black', **tkw)
    
    host.tick_params(axis='x', **tkw)

    lines = [p1, p2, p3, p4, p5, p6]
    host.legend(lines, [l.get_label() for l in lines])
    plt.title('Policy Comparison')
    plt.savefig(prefix + '_policy.png')
    plt.show()
    plt.close()


def main(argv):
    input_file = INPUT_FILE 
    out_dir = OUT_DIR 
    buffer_size = BUFFER_SIZE
    shards_num = SHARDS_NUM
    shard_capacity = SHARD_CAPACITY
    time_interval = TIME_INTERVAL
    inter_cost = 2.0

    try:
        opts, args = getopt.getopt(argv,"hi:o:b:s:c:t:x:",["ifile=","odir=", "buffer=", "shards=", "capacity=", "time=", "inter_cost="])
    except getopt.GetoptError:
        print('compare_policies.py -i <inputfile> -o <outputdir> -b <buffer size> -s <shards_num> -c <shards_capacity> -t <time_interval>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('compare_policies.py -i <inputfile> -o <outputdir> -b <buffer size> -s <shards_num> -c <shards_capacity> -t <time_interval>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_file = arg
        elif opt in ("-o", "--odir"):
            out_dir = arg
        elif opt in ("-b", "--buffer"):
            buffer_size = float(arg)
        elif opt in ("-s", "--shards"):
            shards_num = int(arg)
        elif opt in ("-c", "--capacity"):
            shard_capacity = int(arg)
        elif opt in ("-t", "--time"):
            time_interval = int(arg)
        elif opt in ("-x", "--inter_cost"):
            inter_cost = float(arg)


    print("Using file:", input_file)
    print("Evaluating using", "BUFFER_SIZE:", buffer_size, "SHARDS_NUM:", shards_num, "SHARD_CAPACITY:", shard_capacity, 'TIME_INTERVAL:', time_interval)
    results = {}
    counter = 0
    policy_list = []

    # HashPolicy
    policy_list.append(HashPolicy(shards_num))
    # FuturePolicy
    policy_list.append(FuturePolicy(shards_num, input_file))
    
    # Round-robin policy
    policy_list.append(TheoreticBestPolicy(shards_num))
    
    # ShardSchedulerPolicy
    policy_list.append(ShardSchedulerPolicy(shards_num, intra_shard_cost=1, inter_shard_cost=inter_cost))
    policy_list.append(ShardSchedulerPolicy(shards_num, intra_shard_cost=1, inter_shard_cost=inter_cost, use_allignment=True))

    # SlidingWindowPolicy
    
    #policy_list.append(SlidingWindowPolicy(shards_num, intra_shard_cost=1, inter_shard_cost=inter_cost, window_length=2*buffer_size, load_only=False))
    
    #policy_list.append(SlidingWindowPolicy(shards_num, intra_shard_cost=1, inter_shard_cost=inter_cost, window_length=2*buffer_size, load_only=True))

    
    #policy_list =  [FadingEdgesPolicy(shards_num), HashPolicy(shards_num), DynamicPolicy(shards_num, shard_capacity)]
    #policy_list =  [DynamicPolicy(shards_num, shard_capacity, inter_cost = inter_cost), FadingEdgesPolicy(shards_num), ]
    #policy_list =  [FadingEdgesPolicy(shards_num), HashPolicy(shards_num)]
    dump = []

    for policy in policy_list:
        print("\n Running", policy.__class__.__name__ , " at ", time.time(), file = sys.stderr)
        result = evaluate_policy(input_file, buffer_size, shards_num, shard_capacity, time_interval, policy, inter_cost)
        
        statsList = policy.printStats()
        num_migrations = 0
        if isinstance(policy, DynamicPolicy):
            print('StatsList: ', statsList)
            num_migrations = statsList[0]
            print("Migrations: ", num_migrations)
            num_expensive_migrations = statsList[1]
            print("Number of expensive migrations: ", num_expensive_migrations)
            num_avoidable_expensive_migrations = statsList[2]
            print("Number of avoidable expensive migrations: ", num_avoidable_expensive_migrations)

        if isinstance(policy, DynamicPolicy):
            results['fading'] = policy.d
            results['variant'] = policy.variant
            results['m'] = policy.m

        if isinstance(policy, SlidingWindowPolicy) or isinstance(policy, ShardSchedulerPolicy):
            print('Migrations ordered (not necessarily completed migrations due to capacity): ', statsList[0])
            print('Load:', policy.shard_load)
            #num_migrations = statsList[0]
            
        
        #result['num_migrations'] = num_migrations
        #results[type(policy).__name__ + str(counter)] = result
        
        print(type(policy).__name__ + str(counter), "steps:", result['steps'], 
        "wasted_capacity:", sum(sum(result['wasted_capacity'], [])), 
        "inter", sum(sum(result['inter'], [])), 
        'intra', sum(sum(result['intra'], [])), 
        'load', sum(sum(result['load'], [])))
        result['wasted_capacity'] = sum(sum(result['wasted_capacity'], []))
        result["inter"] = sum(sum(result['inter'], []))
        result["intra"] = sum(sum(result['intra'], []))
        result["load"] = sum(sum(result['load'], []))
        result['policy'] = type(policy).__name__
        result['inter_cost'] = inter_cost
        result['buffer_size'] = buffer_size
        result['input_file'] = input_file
        result['shards_num'] = shards_num
        result['shard_capacity'] = shard_capacity
        
        dump.append(result)
        results[type(policy).__name__ + str(counter)] = result
        counter += 1


        #v, d, m = policy.printStats()
        #if(v == 1):
        #    v1x.append(d)
        #    v1y.append(m)
        #    v1z.append(result['steps'])
        #else:
        #    v2x.append(d)
        #    v2y.append(m)
        #    v2z.append(result['steps'])
    #print("v1x", v1x, file = sys.stderr)
    #print("v1y", v1y, file = sys.stderr)
    #print("v1z", v1z, file = sys.stderr)
    #print("v2x", v2x, file = sys.stderr)
    #print("v2y", v2y, file = sys.stderr)
    #print("v2z", v2z, file = sys.stderr)

    prefix = out_dir + str.split(input_file, '/')[-1] + '_s' + str(shards_num)  + '_b' + str(buffer_size) + '_c' + str(shard_capacity) + '_t' + str(time_interval)
    plot_results(prefix, results)
    print("results", results)

    keys = ['policy', 'steps', 'wasted_capacity', 'inter', 'intra', 'load', 'inter_cost', 'buffer_size', 'inpurt_file', 'shards_num', 'shard_capacity', 'num_migrations', 'input_file', 'latency']
    with open(str(random.randint(0, 10000)) + 'dump.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dump)



if __name__ == "__main__":
   main(sys.argv[1:])
