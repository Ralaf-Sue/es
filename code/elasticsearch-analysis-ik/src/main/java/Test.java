import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.elasticsearch.util.JDBCUtil;

/**
 * @author SuZuQi
 * @title: Test
 * @projectName elasticsearch-analysis-ik
 * @description: TODO
 * @date 2021/4/15
 */
public class Test {
    public static void main(String[] args) {
        String sql = "select * from ik_main_dic where status = 0  ";
        List<Map<String, Object>> result = JDBCUtil.getResult(sql, new String[] {"id", "dict"});
        List<Integer> ids = Optional.ofNullable(result).orElseGet(Collections::emptyList).stream()
            .map(map -> Integer.valueOf(map.get("id").toString())).collect(Collectors.toList());
        Integer size = ids.size();
        if (size > 0) {

            String updateSql = "update ik_main_dic set status=1 where id in ( %s )";
            JDBCUtil.update(updateSql, ids);

        }
    }
}
