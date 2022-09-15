package Number;

import java.util.Random;

public class RandomResponse {
    public final double coinFlipProb = 0.8;
    public double epsilon = 4;

    public void setEpsilon(double value) {
        epsilon = value;
    }

    public boolean coinFlipping() {
        Random rand = new Random();
        double coin = rand.nextDouble();
        if (coin <= coinFlipProb) return false;
        else return !(rand.nextDouble() <= 0.5);
    }

    public boolean randomResponse() {
        Random rand = new Random();
        double prob = Math.exp(epsilon) / (Math.exp(epsilon) + 1);
        double coin = rand.nextDouble();
        return !(coin <= prob);
    }

    public boolean uRandomResponse(){
        Random random = new Random();
        double prob = 1 / Math.exp(epsilon);
        double coin = random.nextDouble();
        return coin <= prob;
    }
}
