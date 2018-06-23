import java.util.ArrayDeque;
import java.util.ArrayList;

public class IdGenerator {

  private String name;
  private ArrayDeque<Integer> identityPool;
  private ArrayList<Integer> identityInUse;
  private static final int INITIAL_POOL_SIZE = 1000;
  private int lowestUnassingedID = 1;

  public IdGenerator(String name) {
    this.name = name;
    identityPool = new ArrayDeque<Integer>(INITIAL_POOL_SIZE);
    identityInUse = new ArrayList<Integer>();
    initIdentityPool();
  }

  private void initIdentityPool() {
    while (lowestUnassingedID <= INITIAL_POOL_SIZE) {
      if (lowestUnassingedID < Integer.MAX_VALUE) {
        identityPool.add(lowestUnassingedID++);
      }
    }
  }

  public Integer getNewId() {

    if (!identityPool.isEmpty()) {
      Integer id = identityPool.pop();
      identityInUse.add(id);
      return id;
    }
    else {
      refillIdPool();
      if (!identityPool.isEmpty()) {
        Integer id = identityPool.pop();
        identityInUse.add(id);
        return id;
      }
      throw new Error("No IDs left");
    }

  }
  
  public void recycleId(Integer id) {
    if(identityInUse.contains(id)) {
      identityInUse.remove(id);
    }
  }

  private void refillIdPool() {
    for (int i = 0; i < INITIAL_POOL_SIZE; i++) {
      if (lowestUnassingedID < Integer.MAX_VALUE) {
        identityPool.add(lowestUnassingedID++);
      }
      else {
        for (int j = 1; j < Integer.MAX_VALUE; j++) {
          if (!identityPool.contains(j)) {
            identityPool.add(j);
          }
        }
        throw new Error("Exception: IDs are available.");
      }
    }
  }

  @Override
  public String toString() {
    return "Identity [name=" + name + ", identityPool=" + identityPool + ", identityInUse=" + identityInUse
        + ", lowestUnassingedID=" + lowestUnassingedID + "]";
  }

}