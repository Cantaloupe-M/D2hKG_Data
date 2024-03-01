
import os
import pandas as pd
from neo4j import GraphDatabase
import json

class MyGraph:
    def __init__(self):
        # 连接服务器neo4j
        self.uri = "bolt://localhost:7687"  # Neo4j 服务器的 URI
        self.username = "neo4j"      # 替换为您的 Neo4j 用户名
        self.password = "11235813"      # 替换为您的 Neo4j 密码
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
    
    def process_node(self,nodes):
        for node_file in nodes:
            label = node_file[:-4]
            filename = os.path.join("node",node_file)
            node_dict_list = pd.read_csv(filename).to_dict(orient="records")
            for node_dict in node_dict_list:
                node_id = node_dict[[item for item in node_dict.keys() if ":ID" in item][0]]
                node_name = node_dict["name"]
                node_dict.pop(f"{label}:ID", None)
                node_dict.pop(f":LABEL", None)
                node_dict.pop("name", None)
                props = node_dict
                insert_node(self.driver, label, node_id, node_name, props)

    def process_edge(self,edges):
        for edge_file in edges:
            rel = edge_file[:-4]
            filename = os.path.join("edge",edge_file)
            edge_df= pd.read_csv(filename)
            for _,row in edge_df.iterrows():
                start_id = row[':START_ID']
                end_id = row[':END_ID']
                rel = row[':TYPE']
                create_relationship(self.driver,start_id,rel,end_id)


def insert_node(driver,label, node_id, name, properties):
    with driver.session() as session:
        # 检查节点是否已经存在
        result = session.run(
            "MATCH (n:{label}) WHERE n.id = $node_id RETURN n".format(label=label),
            node_id=node_id
        )
        existing_node = result.single()
        
        if existing_node:
            print("Node with ID {} already exists. Skipping...".format(node_id))
            return

        # 创建节点
        properties_str = json.dumps(properties,ensure_ascii=False)
        cypher_query = "CREATE (n:{label} {{id: $node_id, name: $name, properties: $properties}})".format(label=label)
        session.run(cypher_query, node_id=node_id, name=name, properties=properties_str)

def create_relationship(driver, head_id, relationship_name, tail_id):
    with driver.session() as session:
    # 检查节点是否已经存在
        result = session.run(
            "MATCH p=(head)-[r:{relationship_name}]->(tail) WHERE head.id = $head_id AND tail.id = $tail_id RETURN p".format(relationship_name=relationship_name),
            head_id=head_id,tail_id=tail_id
        )
        existing_egde = result.single()
    
        if existing_egde:
            print(f"Relationshape ({head_id})-[r:{relationship_name}]->({tail_id}) already exists. Skipping...")
            return

    with driver.session() as session:
        # 创建关系
        cypher_query = f'''
        MATCH (head), (tail) 
        WHERE head.id = $head_id AND tail.id = $tail_id
        CREATE (head)-[r:{relationship_name}]->(tail)
        '''
        session.run(cypher_query, head_id=head_id, relationship_name=relationship_name, tail_id=tail_id)

if __name__=="__main__":
    nodes = os.listdir("node")
    edges = os.listdir("edge")
    g=MyGraph()
    # g.process_node(nodes)
    # g.process_edge(edges)
    g.process_edge(["主要营养成分.csv"])


