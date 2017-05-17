
public class CodingBat {

  public static void main(String[] args) {
    System.out.println("starting main..");
    String returnValue = new CodingBat().missingChar("Hi", 1);
    System.out.println(returnValue);
    System.out.println("ending main..");
  }


  public String missingChar(String str, int n) {
    String part1 = str.substring(0, n);
    String part2 = str.substring(n + 1);
    return part1 + part2;
  }

}
