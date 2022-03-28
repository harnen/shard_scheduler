#!/usr/bin/python3
import pandas as pd
from matplotlib import pyplot as plt
from compare_policies import *
import time
import sys
import matplotlib

font = {'family' : 'normal',
        'weight' : 'bold',
        'size'   : 16}

matplotlib.rc('font', **font)
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

input_file = './input/multi_small.csv'
#input_file = './input/data_hackfs-.csv'
#input_file = './input/balanced.csv'
#input_file = './input/balanced_non_overlap.csv'


#intra_cost
#inter_cost
#buffer_ratio
#shards_num
#shard_capacity
#time_interval


def restore_default():
    global intra_cost, inter_cost, buffer_ratio, shards_num, shard_capacity, time_interval
    intra_cost = 1
    inter_cost = 2
    shards_num = 16
    shard_capacity = 100
    buffer_ratio = 2
    time_interval = 100



results = {}
dump = []
counter = 0


def run():
    policy_list = []
    policy_list.append(HashPolicy(shards_num))
    #policy_list.append(FuturePolicy(shards_num, input_file))
    policy_list.append(MichalPolicy(shards_num, intra_shard_cost=intra_cost, inter_shard_cost=inter_cost))
    policy_list.append(MichalPolicy(shards_num, intra_shard_cost=intra_cost, inter_shard_cost=inter_cost, use_allignment=True))
    #print("Running sliding windows policy with:", shards_num, intra_cost, inter_cost, 2*buffer_ratio*shard_capacity*shards_num )
    #policy_list.append(SlidingWindowPolicy(shards_num, intra_shard_cost=intra_cost, inter_shard_cost=inter_cost, window_length=2*buffer_ratio*shard_capacity*shards_num))

    number_of_lines = len(open(input_file).readlines(  ))

    counter = 0
    for policy in policy_list:
        #continue
        print("\n Running", policy.__class__.__name__ , " at ", time.time(), file = sys.stderr)
        result = evaluate_policy(input_file, buffer_ratio, shards_num, shard_capacity, time_interval, policy, inter_cost)
        statsList = policy.printStats()


        if isinstance(policy, SlidingWindowPolicy) or isinstance(policy, MichalPolicy):
            print('Migrations ordered (not necessarily completed migrations due to capacity): ', statsList[0])
            print('Load:', policy.shard_load)
        
        print(type(policy).__name__ + str(counter), "steps:", result['steps'], 
            "wasted_capacity:", sum(sum(result['wasted_capacity'], [])), 
            "inter", sum(sum(result['inter'], [])), 
            'intra', sum(sum(result['intra'], [])), 
            'load', sum(sum(result['load'], [])),
            'latency', sum(sum(result['latency'],[])))
        result['wasted_capacity'] = sum(sum(result['wasted_capacity'], []))
        result['num_of_tx'] = number_of_lines
        result["inter"] = sum(sum(result['inter'], []))
        result["intra"] = sum(sum(result['intra'], []))
        result["load"] = sum(sum(result['load'], []))
        result["latency"] = sum(sum(result['latency'],[]))
        result["latency"] = result["latency"] / result['num_of_tx']
        result['policy'] = type(policy).__name__+str(counter)
        result['inter_cost'] = inter_cost
        result['buffer_ratio'] = buffer_ratio
        result['input_file'] = input_file
        result['shards_num'] = shards_num
        result['shard_capacity'] = shard_capacity
        result['efficiency'] = result['num_of_tx']/(result['steps']*shard_capacity*shards_num)
        result['inter_ratio'] = result['inter']/(result['num_of_tx'])
        result['with_migrations_ratio'] = (result['inter'] + result['num_migrations'])/(result['num_of_tx'])
        
        counter +=1    
        dump.append(result)


def run_all():
    global inter_cost, shards_num, shard_capacity, buffer_ratio
    restore_default()
    for i in [2, 4, 6, 10]:
        inter_cost = i
        run()

    restore_default()
    for i in [2, 4, 5, 10, 30]:
        shards_num = i
        run()

    restore_default()
    for i in [100, 500, 1000]:
        shard_capacity = i
        run()

    restore_default()
    for i in [1, 2, 10]:
        buffer_ratio = i
        run()

    df = pd.DataFrame(dump)
    print(df)
    df.to_csv('dump.csv')
    print("#################################")

def select_results_with_default_params(df, exclude = None):
    restore_default()
    params = ['inter_cost', 'buffer_ratio', 'shards_num', 'shard_capacity']
    defaults = {}
    defaults['inter_cost'] = inter_cost
    defaults['buffer_ratio'] = buffer_ratio
    defaults['shards_num'] = shards_num
    defaults['shard_capacity'] = shard_capacity

    if(exclude != None):
        params.remove(exclude)
    for p in params:
        df = df.loc[df[p] == defaults[p]]
    return df

def plot_feature(ax, df, label, y, x_title, y_title, key_suffix = None):
    colors = ['red', 'green', 'blue', 'orange', 'black']#['0.1', '0.5', '0.8']
    styles = ['solid', 'dashed', 'dashdot']
    counter = 0
    for key, group in df.groupby('policy'):
        #if(key == 'SlidingWindowPolicy'):
        #    continue
        if(key == 'MichalPolicy'):
            key = "OMO"
        if(key == 'HashPolicy'):
            key = "HashBased"
        if(key_suffix != None):
            key += key_suffix
        group_specific = select_results_with_default_params(group, label) 
        print(group_specific)
        print("x_old", group_specific[label])
        print("y_old", group_specific[y])
        #sort points
        x_new, y_new = zip(*sorted(zip(group_specific[label], group_specific[y])))
        print("x_new", x_new)
        print("y_new", y_new)
        ax.plot(x_new, y_new, label=key, c = colors[counter%len(colors)], linestyle = styles[counter%len(styles)], linewidth = 5)
        ax.scatter(x_new, y_new, c = colors[counter%len(colors)], linewidth = 5)
        #ax.plot(group_specific[label], group_specific[y], label=key, linewidth = 5)
        counter += 1
    ax.set_xlabel(x_title)
    ax.set_ylabel(y_title)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.legend()
    #plt.tight_layout()
    #plt.savefig('./output/'+ x_title + y_title +'.png')

def analyze(input_file = 'dump.csv'):
    df = pd.read_csv(input_file)
    print(df)
    #df.drop(columns=[1], axis=1)
    df.drop(df.columns[[0]], axis=1, inplace=True)
    print(df)
    df.drop_duplicates(inplace=True)
    print(df['num_of_tx'], df['inter'], df['inter_ratio'])
    #quit()
    
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'inter_cost', 'steps', 'Cross-shard tx cost', '#Blocks')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shards_num', 'steps', '#Shards', '#Blocks')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shard_capacity', 'steps', 'Shard capacity', '#Blocks')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'buffer_ratio', 'steps', 'Mempoll/blockchain capacity ratio', '#Blocks')
    #plt.show()


    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'inter_cost', 'efficiency', 'Cross-shard tx cost', 'Efficiency')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shards_num', 'efficiency', '#shards', 'Efficiency')
    #fig, ax = plt.subplots(figsize=(10, 4))
    #plot_feature(ax, df, 'shard_capacity', 'tps', 'Shard capacity', 'Efficiency')
    #fig, ax = plt.subplots(figsize=(10, 4))
    #plot_feature(ax, df, 'buffer_ratio', 'tps', 'Mempoll/blockchain capacity ratio', 'Efficiency')
    
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'inter_cost', 'inter_ratio', 'Cross-shard tx cost', 'cross-shard tx ratio')
    plot_feature(ax, df, 'inter_cost', 'with_migrations_ratio', 'Cross-shard tx cost', 'cross-shard tx ratio', ' with Migrations')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shards_num', 'inter_ratio', '#shards', 'cross-shard tx ratio')
    plot_feature(ax, df, 'shards_num', 'with_migrations_ratio', '#shards', 'cross-shard tx ratio', ' with Migrations')
    #fig, ax = plt.subplots(figsize=(10, 4))
    #plot_feature(ax, df, 'shard_capacity', 'inter_ratio', 'Shard capacity', 'cross-shard tx ratio')
    #fig, ax = plt.subplots(figsize=(10, 4))
    #plot_feature(ax, df, 'buffer_ratio', 'inter_ratio', 'Mempoll/blockchain capacity ratio', 'cross-shard tx ratio')

    #plt.show()
    #quit()
    #fig, axes = plt.subplots(2, 2)
    #plot_feature(axes[0, 0], df, 'inter_cost', 'num_migrations', 'Cross-shard tx cost', '#Migrations')
    #plot_feature(axes[0, 1], df, 'shards_num', 'num_migrations', '#shards', '#Migrations')
    #plot_feature(axes[1, 0], df, 'shard_capacity', 'num_migrations', 'Shard capacity', '#Migrations')
    #plot_feature(axes[1, 1], df, 'buffer_ratio', 'num_migrations', 'Mempoll/blockchain capacity ratio', '#Migrations')
    
    #fig, axes = plt.subplots(2, 2)
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'inter_cost', 'wasted_capacity', 'Cross-shard tx cost', 'Wasted capacity')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shards_num', 'wasted_capacity', '#shards', 'Wasted capacity')
    #plot_feature(axes[1, 0], df, 'shard_capacity', 'wasted_capacity', 'Shard capacity', 'Wasted capacity')
    #plot_feature(axes[1, 1], df, 'buffer_ratio', 'wasted_capacity', 'Mempoll/blockchain capacity ratio', 'Wasted capacity')

    #fig, axes = plt.subplots(2, 2)
    #plot_feature(axes[0, 0], df, 'inter_cost', 'load', 'Cross-shard tx cost', 'Total load')
    #plot_feature(axes[0, 1], df, 'shards_num', 'load', '#shards', 'Total load')
    #plot_feature(axes[1, 0], df, 'shard_capacity', 'load', 'Shard capacity', 'Total load')
    #plot_feature(axes[1, 1], df, 'buffer_ratio', 'load', 'Mempoll/blockchain capacity ratio', 'Total load')

    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'inter_cost', 'latency', 'Cross-shard tx cost', 'Average latency')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shards_num', 'latency', '#shards', 'Average latency')
    
    plt.show()

run_all()
analyze()
