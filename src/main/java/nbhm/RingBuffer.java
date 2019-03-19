package nbhm;

import java.util.function.Supplier;

/**
 * Simple Ring Buffer implementation.
 */
public class RingBuffer<T> {
    private final int capacity;
    private final T[] elements;

    private int writePos;
    private int available;

    @SuppressWarnings("unchecked")
    public RingBuffer(final int capacity, final Supplier<T> constructor) {
        this.capacity = capacity;
        this.elements = (T[])new Object[capacity];
        for (int i = 0; i < capacity; i++) {
            elements[i] = constructor.get();
        }

        this.available = capacity;
        this.writePos = capacity;
    }

    public T takeEntry() {
        if(available == 0){
            return null;
        }
        int nextSlot = writePos - available;
        if(nextSlot < 0){
            nextSlot += capacity;
        }
        final T nextObj = elements[nextSlot];
        available--;
        return nextObj;
    }

    public void returnEntry(final T element) {
        if(available < capacity){
            if(writePos >= capacity){
                writePos = 0;
            }
            elements[writePos] = element;
            writePos++;
            available++;
        }
    }
}
