package Day2;

public class Test1 {
    public static void main(String[] args) {
        Integer x = 100;
        int ints = x.intValue();
        int ints2 = 100;
        Integer y = Integer.valueOf(ints2);
        // i >= -128 && i <= Integer.IntegerCache.high ? Integer.IntegerCache.cache[i + 128] : new Integer(i);
        Integer x1 = 10;
        Integer x2 = 10;
        int x3 = 10;
        System.out.println(((Object) x3 instanceof Integer));
        System.out.println(x1==(x3));
        Long x4 =13L;
        System.out.println(x2 .equals(11));//f
        System.out.println(x4 == (7+6));//t
        System.out.println(x4.equals((7+6)));//f
    }
}
