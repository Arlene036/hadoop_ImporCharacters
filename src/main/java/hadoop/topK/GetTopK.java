package hadoop.topK;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class GetTopK {

    public static List<CharacterImportanceNode> topKMax(List<CharacterImportanceNode> nums, int k){
        //寻找前k个最小数，因此将小顶堆大小定义为k
        PriorityQueue<CharacterImportanceNode> pq = new PriorityQueue<>(k);
        for(int i=0; i<nums.size(); i++){
            if(i<k){
                pq.offer(nums.get(i));	//前k个数，直接入堆
            }else if(nums.get(i).getScore()>pq.peek().getScore()){	//如果当前元素比堆顶元素大
                pq.poll();	//说明堆顶元素（堆中最小元素）一定不属于前k大的数，出堆
                pq.offer(nums.get(i));	//当前元素有可能属于前k大，入堆
            }
        }

        List<CharacterImportanceNode> ans = new ArrayList<>();
        while(!pq.isEmpty()){
            ans.add(pq.poll());
        }
        return ans;
    }

}
