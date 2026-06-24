public class TestMath {
    public static void main(String[] args) {
        long writePosition = 0;
        int messageSize = 116;
        int maxFileSize = 10485760;
        int count = 0;
        while (writePosition + messageSize <= maxFileSize) {
            writePosition += messageSize;
            count++;
        }
        System.out.println("Message count per segment: " + count);
        System.out.println("Write position after all messages: " + writePosition);
        long lastMessageStart = writePosition - messageSize;
        System.out.println("Last message start pos: " + lastMessageStart);
    }
}
