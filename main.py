from neo4j import GraphDatabase
import pandas as pd
from tqdm import tqdm
import os.path as osp
from timeit import default_timer as timer
import datetime
import os
import getpass


DATASETS = ["condmat", "gnutella"]
NEO4J_HOME = os.environ["NEO4J_HOME"]
IMPORT_DIR_PATH = osp.join(NEO4J_HOME, "import")

def get_graph_path(dataset):
    return "datasets/%s_graph.txt" % (dataset)

def get_graph_df(dataset):
    # 第二行开始读取，第一行是节点数和边数
    V,E = [int(s[2:]) for s in open(get_graph_path(dataset)).readline().split()]
    df = pd.read_csv(get_graph_path(dataset), sep="\t", header=0, skiprows=1)
    print(V, E)
    assert len(df) == 2*E # 存储有向边
    return V,E,df

if __name__ == "__main__":
    uri = "neo4j://localhost:7687"
    auth = ("neo4j", getpass.getpass("connecting to neo4j, password: "))
    
    for i in range(5):
        try:
            with GraphDatabase.driver(uri, auth=auth) as driver:
                driver.verify_connectivity()
            break
        except Exception as e:
            print(e)
            print("connecting to neo4j failed, try again")
            auth = ("neo4j", getpass.getpass("connecting to neo4j, password: "))
            if i == 4:
                print("connecting to neo4j failed")
                exit()

    with GraphDatabase.driver(uri, auth=auth) as driver:
        for dataset in DATASETS:
            # dataset = "condmat"
            seed = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            V,E,df = get_graph_df(dataset)
            
            # 清空数据库
            with driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")
                result = session.run("MATCH (n:Node) RETURN count(n)")
                print(result.single()[0])
                result = session.run("MATCH ()-[r:Edge]->() RETURN count(r)")
                print(result.single()[0])
                    
            nodes = pd.DataFrame({"id": range(V)})
            nodes.to_csv(osp.join(IMPORT_DIR_PATH, dataset + "_nodes.csv"), index=False)
            edges = df[["Vetex_1", "Vertex_2", "Jaccard_weight*100"]]
            edges.columns = ["a", "b", "w"]
            edges.to_csv(osp.join(IMPORT_DIR_PATH, dataset + "_edges.csv"), index=False)
            
            # 将 df 导入 neo4j
            with driver.session() as session:
                session.run(r"LOAD CSV WITH HEADERS FROM 'file:///" +dataset + r"_nodes.csv' AS line CREATE (a:Node {id: toInteger(line.id)})")
                session.run(r"LOAD CSV WITH HEADERS FROM 'file:///" +dataset + r"_edges.csv' AS line CREATE (a:Node {id: toInteger(line.a)}), (b:Node {id: toInteger(line.b)}) CREATE (a)-[r:Edge {weight: toFloat(line.w)}]->(b)")
                    
            results = []
            with driver.session() as session:
                # 输出 neo4j 中的节点数和边数
                result = session.run("MATCH (n:Node) RETURN count(n)")
                print(result.single()[0])
                result = session.run("MATCH ()-[r:Edge]->() RETURN count(r)")
                print(result.single()[0])
                
                exit(0)
                
                query_df = pd.read_csv("datasets/exp_case_%s.csv" % (dataset))
                for id, (change_v1,change_v2,change_w0,change_w1,change_time,query_v1,query_v2,query_time) in tqdm(enumerate(query_df.values), total=len(query_df)):
                    # 修改权重
                    tic = timer()
                    result = session.run(r"""
                    MATCH (a:Node {id: $a})-[r:Edge]->(b:Node {id: $b})
                    SET r.weight = $weight
                    """, a=change_v1, b=change_v2, weight=change_w1)
                    toc = timer()
                    update_time = toc-tic
                    tqdm.write("update: %s" % update_time)
                    
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
                    tqdm.write("project: %s" % project_time)
                    
                    # 查询最短路径
                    tic = timer()
                    result = session.run(r"""
                    MATCH (a:Node {id: $a}), (b:Node {id: $b})
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
                    tqdm.write("query: %s" % query_time)

                    results.append([update_time, project_time, query_time])
                    
                    # 删除投影
                    session.run(r"""CALL gds.graph.drop('graph_%s_%s')""" % (seed, id))
                    
                results = pd.DataFrame(results, columns=["update_time", "project_time", "query_time"])
                results.to_csv("results/exp_case_%s.csv" % (dataset), index=False)

    # 后处理
    df = pd.DataFrame(columns=["dataset", "update_time", "project_time", "query_time"])
    for dataset in DATASETS:
        query_df = pd.read_csv("datasets/exp_case_%s.csv" % (dataset))
        result_df = pd.read_csv("results/exp_case_%s.csv" % (dataset))
        query_df["neo4j_update_time"] = result_df["update_time"]
        query_df["neo4j_project_time"] = result_df["project_time"]
        query_df["neo4j_query_time"] = result_df["query_time"]
        query_df.to_csv("results/neo4j_exp_case_%s.csv" % (dataset), index=False)
        mean_update_time = query_df["neo4j_update_time"].mean()
        mean_project_time = query_df["neo4j_project_time"].mean()
        mean_query_time = query_df["neo4j_query_time"].mean()
        df.loc[len(df)] = [dataset, mean_update_time, mean_project_time, mean_query_time]
    print(df)