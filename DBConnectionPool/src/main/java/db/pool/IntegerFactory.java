package db.pool;

import java.util.ArrayList;
import java.util.List;

class IntegerFactory
{
    private static List<Integer> list = new ArrayList<Integer>(100);

    static
    {
        for (int i = 0; i < 100; i++)
            list.add(new Integer(i));
    }

    static Integer get(int value)
    {
        return ((value >= list.size()) ? new Integer(value) : (Integer) list
            .get(value));
    }

}
