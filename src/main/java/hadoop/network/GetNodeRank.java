package hadoop.network;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.algorithm.PageRank;
import org.graphstream.algorithm.generator.DorogovtsevMendesGenerator;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;

public class GetNodeRank {

    //input format:node
    //Ali
    public static double getNodeRank(Graph graph, String node){
        Node n = graph.getNode(node);

        PageRank pageRank = new PageRank();
        pageRank.setVerbose(false);
        pageRank.init(graph);

        return pageRank.getRank(n);
    }

    //input format:graph(一本书
    //Ali	Roger,Marley,Poor,Fezziwig,Idol,Jack,Robin,Dick,
    //Bob	Marley,Merry,Tiny,Fred,Scrooge,Lord,Yellow,Holly,This,Tim,Martha,Joe,Robert,Will,Peter,Pray,Caroline,Cratchit,Jacob,
    public static Graph formGraph(String graph){
        Graph g = new SingleGraph("g");
        String[] lines = graph.split("\n");

        //先添加所有node
        for(String line:lines){
            String[] temp = line.split("\t");
            g.addNode(temp[0]);
            Node n = g.getNode(temp[0]);
            n.setAttribute("marked",false);
        }

        for(String line:lines){
            String[] temp = line.split("\t");
            Node n = g.getNode(temp[0]);
            System.out.println(temp[0]+"-----------------------GNR line 43-------------------");

            boolean d = (Boolean) n.getAttribute("marked");
            if(!d){
                String[] neighbors = temp[1].split(",");
                for(String neighbor : neighbors){
                    Node n1 = g.getNode(neighbor);

                    if(n==null || n1 == null || neighbor == null || temp[0] == null) {
                        System.out.println("null!!!------------GNR 51-----------------");
                        continue;
                    }

                    g.addEdge(temp[0]+neighbor,n,n1); //why null pointer
                    n1.setAttribute("marked",true);
                }
            }

        }

        return g;

    }
}
