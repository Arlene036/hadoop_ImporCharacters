package hadoop.topK;

public class CharacterImportanceNode implements Comparable{
    private String name;
    private double score;
    public CharacterImportanceNode(String name, double score){
        this.name = name;
        this.score = score;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public int compareTo(Object o) {
        CharacterImportanceNode oo = (CharacterImportanceNode) o;
        return Double.compare(this.getScore(), oo.getScore());
    }
}
