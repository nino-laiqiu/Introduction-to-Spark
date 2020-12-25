package Day3;

public class Test2 {
    public static void main(String[] args) {
        People people1 = People.getInstance();
        People people2 = People.getInstance();
        System.out.println(people1 == people2);
        Singleton singleton1 = Singleton.getInstance();
        Singleton singleton2 = Singleton.getInstance();
        System.out.println(singleton1 == singleton2);
        User user1 = User.getInstance();
        User user2 = User.getInstance();
        System.out.println(user1 == user2);
        Student student1 = Student.getInstance();
        Student student2 = Student.getInstance();
        System.out.println(student1 == student2);
    }
}

//懒汉式
class People {
    private static People people = null;

    public synchronized static People getInstance() {
        if (people == null) {
            people = new People();
        }
        return people;

    }
}

//饿汉式,天生线程安全
class Singleton {
    private Singleton() {
    }

    private static Singleton single = new Singleton();

    //静态工厂方法
    public synchronized static Singleton getInstance() {
        return single;
    }
}

//双重检验,懒汉式
class User {
    private volatile static User user = new User();

    public static User getInstance() {
        if (user == null) {
            synchronized (User.class) {
                if (user == null) {
                    user = new User();
                }
            }
        }
        return user;
    }
}

//使用静态内部类
class Student {
    private static class StudentLazyHolder {
        private static final Student INSTANCE = new Student();
    }

    public static final Student getInstance() {
        return StudentLazyHolder.INSTANCE;
    }
}

