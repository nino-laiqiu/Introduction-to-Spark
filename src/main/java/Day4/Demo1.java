package Day4;

public class Demo1 {
    public static void main(String[] args) {
        Vehicle vehicle = new Vehicle();
        vehicle.run("火车");
        vehicle.run("飞机");


    }
}

//发现问题,飞机不会在路上运输,要改变代码

class Vehicle {
    public void run(String vehicle) {
        System.out.println(vehicle + "在路上运输");
    }
}

//TODO 一个类一个方法 修改时如果遵循单一职责原则 问题类变多了,开销变大

class Vehicle1 {
    public void run(String vehicle) {
        System.out.println(vehicle + "在路上运输");
    }

}

class Vehicle2 {
    public void runWater(String vehicle) {
        System.out.println(vehicle + "在水上运输");
    }
}

// TODO 修改代码 但是会发生职责扩散,存在潜在风险

class Vehicle3 {
    public void run(String vehicle) {
        if (vehicle != "飞机") {
            System.out.println(vehicle + "在路上运输");
        } else {
            System.out.println(vehicle + "在天空运输");
        }
    }
}

//TODO 改进

class Vehicle4 {
    public void run(String vehicle) {
        System.out.println(vehicle + "在路上运输");
    }

    public void runWater(String vehicle) {
        System.out.println(vehicle + "在水上运输");
    }

}