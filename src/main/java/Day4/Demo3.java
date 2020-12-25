package Day4;

import scala.annotation.meta.param;

/**
 * @author hp
 */
public class Demo3 {
    public static void main(String[] args) {
        People people = new People();
        people.getMessage(new Email());
        people.getMessage(new Weixin());
    }
}

interface IReceiver {
    String send();
}

class Email implements IReceiver {

    @Override
    public String send() {
        return "电子邮箱信息";
    }
}

class Weixin implements IReceiver {

    @Override
    public String send() {
        return "微信的信息";
    }
}


class People {
    public void getMessage(IReceiver receiver) {
        System.out.println(receiver.send());
    }
}