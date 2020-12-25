package Day4;

public class Demo2 {
    public static void main(String[] args) {
        Bird bird = new Bird();
        bird.eat1(new FlyingAnimal());

    }
}

interface Animal {
    void eat1();

    void eat2();

    void eat3();

    void eat4();

    void eat5();
}

// TODO 4 5 没用到怎么改进

class FlyingAnimal implements Animal {

    @Override
    public void eat1() {
        System.out.println("种子");
    }

    @Override
    public void eat2() {
        System.out.println("水果");
    }

    @Override
    public void eat3() {
        System.out.println("肉");
    }

    @Override
    public void eat4() {
        System.out.println("eat");
    }

    @Override
    public void eat5() {
        System.out.println("*");
    }
}

// TODO 依赖于FlyingAnimal中的1 2 3

class Bird {
    public void eat1(Animal animal) {
        animal.eat1();
    }

    public void eat2(Animal animal) {
        animal.eat2();
    }

    public void eat3(Animal animal) {
        animal.eat3();
    }
}

//TODO 改进,接口隔离原则

interface Animal2 {
    void eat1();
}

interface Animal3 {
    void eat2();

    void eat3();
}

interface Animal4 {
    void eat4();

    void eat5();
}

