public class FrestoStopWatch {
    public long startTime;
    public long endTime;

    public FrestoStopWatch() {
        startTime = System.currentTimeMillis();
    }

    public long start() {
        startTime = System.currentTimeMillis();
        return this.startTime;
    }

    public long stop() {
        endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		startTime = endTime;
        return elapsedTime;
    }

    public long lap() {
        endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		startTime = endTime;
        return elapsedTime;
    }

    public int getElapsedTime() {
        return (int)(endTime - startTime);
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }
}
