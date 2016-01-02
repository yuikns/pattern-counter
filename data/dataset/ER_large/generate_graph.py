import random as ran
import networkx as nx
import Queue
import matplotlib.pyplot as plt
n=10   # number of nodes
p=0.5   # edge selection probability
G=nx.erdos_renyi_graph(n,p)

print "#nodes = ", G.number_of_nodes()
print "#edges = ", G.number_of_edges()

L=20    # number of logs
influence_probability = 0.35 # activity probability
seed_id = ran.randint(0,n-1)
q = Queue.Queue()
l = 0 
q.put((seed_id,0))
G.node[seed_id]['label'] = '0'
logs = []
logs.append((seed_id, 0))
while(not q.empty() and l < L):
	l+=1
	(node_id, t) = q.get()
	neighbors = G.neighbors(node_id)
	if len(neighbors) == 0:
		node_id = ran.randint(0,n-1)
		if G.node[node_id].get('label') != None:
			G.node[node_id]['label'] = G.node[node_id].get('label')+"-"+str(t+1) 
		else:
			G.node[node_id]['label'] = str(t+1) 
		q.put((node_id, t+1))
		logs.append((node_id, t+1))
		print len(logs)
	else:
		for neighbor_id in neighbors:
			x = ran.random()
			if x <= influence_probability:
				if G.node[neighbor_id].get('label') != None:
					G.node[neighbor_id]['label'] = G.node[neighbor_id].get('label')+"-"+str(t+1) 
				else:
					G.node[neighbor_id]['label'] = str(t+1) 				

				q.put((neighbor_id, t+1))
				logs.append((neighbor_id, t+1))
	
for i in range(G.number_of_nodes()):
	print i, " ", G.node[i].get('label') 


f = open("graph.txt","w")
f.write(str(G.number_of_nodes())+"\t"+str(G.number_of_edges())+"\n")
for edge in G.edges():
	f.write(str(edge[0])+"\t"+str(edge[1])+"\t1.0\n")
f.close()

f = open("node_dict.txt", "w")
for node in range(n):
	f.write(str(node)+"\t"+str(node)+"\n")
f.close()

print "#logs = ", len(logs)
f = open("logs.txt", "w")
for log in logs:
	f.write(str(log[0])+"\t"+str(log[1])+"\n")
f.close()
nx.draw(G, with_labels=True)
# nx.draw_networkx_labels(G,pos=nx.spring_layout(G))
plt.show()

