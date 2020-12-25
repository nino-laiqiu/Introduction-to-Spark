package Day4;

public class Demo4 {
    public static void main(String[] args) {
        Pig pig = new Pig();
        pig.setmaterials(new Apple());
        pig.show();

    }
}

interface Zoon {
    void show();

    void setmaterials(Materials materials);
}

interface Materials {
    void play();

}

class Pig implements Zoon {
    Materials materials;

    @Override
    public void show() {
        materials.play();
    }

    @Override
    public void setmaterials(Materials materials) {
        this.materials = materials;
    }
}

class Apple implements Materials {

    @Override
    public void play() {
        System.out.println("++++");
    }
}
