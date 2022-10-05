package hadoop.network;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.algorithm.PageRank;
import org.graphstream.algorithm.generator.DorogovtsevMendesGenerator;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;

//https://graphstream-project.org/doc/Tutorials/Getting-Started/
public class Tutorial {
    public static void main(String args[]) {
        Graph graph = new SingleGraph("Tutorial 1");

        graph.addNode("A" );
//        graph.addNode("A" );
        graph.addNode("B" );
        graph.addNode("C" );
        graph.addEdge("BC", "B", "C");
//        graph.addEdge("B", "C", "B");
        graph.addEdge("CA", "C", "A");

        Node node = graph.getNode("A");

        PageRank pageRank = new PageRank();
        pageRank.setVerbose(false);
        pageRank.init(graph);

        double rank = pageRank.getRank(node);

        node.setAttribute("ui.label", String.format("%.2f%%", rank * 100));
        node.setAttribute("already",true);
        System.out.println(rank);
//        boolean b = (Boolean) graph.getNode("A").getAttribute("already");
//        System.out.println(b);
        //page rank algorithm
    }

}
