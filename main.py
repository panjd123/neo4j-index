from neo4j import GraphDatabase
import pandas as pd
from tqdm import tqdm
import os.path as osp
from timeit import default_timer as timer
import datetime
import os
import getpass
import time


DATASETS = ["condmat", "gnutella"]
# DATASETS = ["condmat", "gnutella", "amazon", "book", "hyves", "skitter"]
NEO4J_HOME = os.environ["NEO4J_HOME"]
IMPORT_DIR_PATH = osp.join(NEO4J_HOME, "import")
INIT_CONF_PATH = osp.join(NEO4J_HOME, "conf", "neo4j.conf.init")
CONF_PATH = osp.join(NEO4J_HOME, "conf", "neo4j.conf")
DATA_DIR_PATH = osp.join(NEO4J_HOME, "data", "databases")
TRANS_DIR_PATH = osp.join(NEO4J_HOME, "data", "transactions")
# EXP_FILE = "exp_case_%s.csv"
EXP_FILE = "exp_case_random_%s.csv"

def get_graph_path(dataset):
    return "datasets/%s_graph.txt" % (dataset)

def get_graph_df(dataset):
    # 第二行开始读取，第一行是节点数和边数
    V,E = [int(s[2:]) for s in open(get_graph_path(dataset)).readline().split()]
    df = pd.read_csv(get_graph_path(dataset), sep="\t", header=0, skiprows=1)
    assert len(df) == 2*E # 存储有向边
    return V,E,df

if __name__ == "__main__":
    uri = "neo4j://localhost:7687"
    auth = ("neo4j", getpass.getpass("connecting to neo4j, password: "))

    for dataset in DATASETS:
        V,E,df = get_graph_df(dataset)
        print("dataset: {}, V={}, E={}".format(dataset, V, E))
        
        # 创建图
        force = False
        exits = os.path.exists(osp.join(DATA_DIR_PATH, dataset)) and os.path.exists(osp.join(TRANS_DIR_PATH, dataset))
        
        if not exits or force:
            if os.path.exists(osp.join(DATA_DIR_PATH, dataset)):
                os.system("rm -rf %s" % (osp.join(DATA_DIR_PATH, dataset)))
            if os.path.exists(osp.join(TRANS_DIR_PATH, dataset)):
                os.system("rm -rf %s" % (osp.join(TRANS_DIR_PATH, dataset)))
            nodes = pd.DataFrame({":ID": range(V)})
            nodes[":LABEL"] = "Node"
            edges = df[["Vetex_1", "Vertex_2", "Jaccard_weight*100"]]
            edges.columns = [":START_ID", ":END_ID", "weight:double"]
            edges[":TYPE"] = "Edge"
            nodes.to_csv(osp.join(IMPORT_DIR_PATH, dataset + "_nodes.csv"), index=False, sep=",")
            edges.to_csv(osp.join(IMPORT_DIR_PATH, dataset + "_edges.csv"), index=False, sep=",")
            os.system("neo4j-admin database import full --nodes %s --relationships %s %s" % (osp.join(IMPORT_DIR_PATH, dataset + "_nodes.csv"), osp.join(IMPORT_DIR_PATH, dataset + "_edges.csv"), dataset))
        
        os.system("cp %s %s" % (INIT_CONF_PATH, CONF_PATH))
        with open(CONF_PATH, "a") as f:
            f.write("dbms.default_database=%s\n" % (dataset))
        os.system("neo4j-admin server restart")
        print("Waiting for neo4j to ready...")
        while True:
            try:
                with GraphDatabase.driver(uri, auth=auth) as driver:
                    driver.verify_connectivity()
                break
            except Exception as e:
                if str(e)=="Unable to retrieve routing information":
                    time.sleep(1)
                else:
                    raise e
        
        with GraphDatabase.driver(uri, auth=auth, database=dataset) as driver:
            total_tic = timer()
            seed = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            
            results = []
            with driver.session() as session:
                # 输出 neo4j 中的节点数和边数
                result = session.run("MATCH (n) RETURN count(n)")
                nodes = result.single()[0]
                result = session.run("MATCH ()-[r]->() RETURN count(r)")
                edges = result.single()[0]
                print("neo4j: V={}, E={}".format(nodes, edges))
                assert nodes == V
                assert edges == 2*E
                
                query_df = pd.read_csv(osp.join("expsets", EXP_FILE % (dataset)))
                # for id, (change_v1,change_v2,change_w0,change_w1,change_time,query_v1,query_v2,query_time) in tqdm(enumerate(query_df.values), total=len(query_df)):
                for id, (change_v1,change_v2,change_w0,change_w1,query_v1,query_v2,query_time) in tqdm(enumerate(query_df.values), total=len(query_df)):
                    # 修改权重
                    tic = timer()
                    result = session.run(r"""
                    MATCH (a {ID: $a})-[r]->(b {ID: $b})
                    SET r.weight = $weight
                    """, a=change_v1, b=change_v2, weight=change_w1)
                    toc = timer()
                    update_time = toc-tic
                    # tqdm.write("update: %s" % update_time)
                    
                    # 重新投影  
                    tic = timer()
                    session.run(r"""
                    CALL gds.graph.project(
                        'graph_%s_%s',
                        'Node',
                        'Edge',
                        {
                            relationshipProperties: 'weight'
                        }
                    )
                    """ % (seed,id))
                    toc = timer()
                    project_time = toc-tic
                    # tqdm.write("project: %s" % project_time)
                    
                    # 查询最短路径
                    tic = timer()
                    result = session.run(r"""
                    MATCH (a {ID: $a}), (b {ID: $b})
                    CALL gds.shortestPath.dijkstra.stream('graph_%s_%s', {
                        sourceNode: a,
                        targetNode: b,
                        relationshipWeightProperty: 'weight'
                    })
                    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                    RETURN index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                    """ % (seed, id), a=query_v1, b=query_v2)
                    toc = timer()
                    query_time = toc-tic
                    query_result = result.single()
                    # tqdm.write("query: %s" % query_time)
                    
                    tic = timer()
                    session.run(r"""CALL gds.graph.drop('graph_%s_%s')""" % (seed, id))
                    toc = timer()
                    drop_time = toc-tic
                    # tqdm.write("drop: %s" % drop_time)
                    results.append([update_time, project_time, query_time, drop_time])
                    
                # session.run(r"""CALL db.checkpoint()""")
                total_toc = timer()
                total_time = total_toc-total_tic
                print("%s total time: %s" % (dataset, total_time))
                results = pd.DataFrame(results, columns=["update_time", "project_time", "query_time", "drop_time"])
                total_true_query_time = results["project_time"].sum()+results["query_time"].sum()+results["drop_time"].sum()
                total_true_update_time = total_time-total_true_query_time
                print("%s true update time: %s" % (dataset, total_true_update_time/len(results)))
                print("%s query time: %s" % (dataset, (total_true_query_time)/len(results)))
                
                results.to_csv(osp.join("results", EXP_FILE % (dataset)), index=False)

    # 后处理
    df = pd.DataFrame(columns=["dataset", "update_time", "project_time", "query_time", "drop_time"])
    for dataset in DATASETS:
        query_df = pd.read_csv(osp.join("expsets", EXP_FILE % (dataset)))
        result_df = pd.read_csv(osp.join("results", EXP_FILE % (dataset)))
        query_df["neo4j_update_time"] = result_df["update_time"]
        query_df["neo4j_project_time"] = result_df["project_time"]
        query_df["neo4j_query_time"] = result_df["query_time"]
        query_df["neo4j_drop_time"] = result_df["drop_time"]
        query_df.to_csv(osp.join("results", "neo4j_" + EXP_FILE % (dataset)), index=False)
        mean_update_time = query_df["neo4j_update_time"].mean()
        mean_project_time = query_df["neo4j_project_time"].mean()
        mean_query_time = query_df["neo4j_query_time"].mean()
        mean_drop_time = query_df["neo4j_drop_time"].mean()
        df.loc[len(df)] = [dataset, mean_update_time, mean_project_time, mean_query_time, mean_drop_time]
    print(df)