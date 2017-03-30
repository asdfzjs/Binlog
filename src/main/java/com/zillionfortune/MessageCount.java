import com.alibaba.otter.canal.protocol.CanalEntry;
import org.omg.CORBA.INTERNAL;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by daniel on 17-1-18.
 */
public class MessageCount {
    /**
     *
     * @param columnList
     * @param mysqlCon
     * @param businessType 
     */
    public void assetAdd2FutureAssetSituation(List<CanalEntry.Column> columnList,Connection mysqlCon,int businessType,int flowDirection){

        String borrowingEndDate = "";
        String cashierDate = "";
        String borrowingStartDate = "";
        String loanDate = "";

        BigDecimal loanAmount = null;
        BigDecimal borrowingMoney = null;
        BigDecimal borrowingFate = null;
        BigDecimal cashierSum = null;

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        simpleDateFormat.setLenient(false);

        for (CanalEntry.Column column : columnList) {
            if("borrowingEndDate".equals(column.getName())){
                borrowingEndDate = column.getValue();
            }
            if("borrowingMoney".equals(column.getName())){
                borrowingMoney = new BigDecimal(column.getValue());
            }
            if("borrowingFate".equals(column.getName())){
                borrowingFate = new BigDecimal(column.getValue());
            }
            if("cashierDate".equals(column.getName())){
                cashierDate = column.getValue();
            }
            if("cashierSum".equals(column.getName())){
                cashierSum = new BigDecimal(column.getValue());
            }
            if("borrowingStartDate".equals(column.getName())){
                borrowingStartDate = column.getValue();
            }
            if("loanDate".equals(column.getName())){
                loanDate = column.getValue();
            }
            if("loanAmount".equals(column.getName())){
                loanAmount = new BigDecimal(column.getValue());
            }

        }

        String querySql = "select assets_forecast_inflow,assets_forecast_outflows,assets_asset_residue,assets_net_flows,total_inflow,total_outflow,total_net_flows,total_asset_residue" +
                " from FutureAssetSituation where transaction_date = ? and business_type = ?";

        String insertSql = "insert into FutureAssetSituation(assets_forecast_inflow,assets_forecast_outflows," +
                "assets_net_flows,total_inflow,total_outflow," +
                "total_net_flows,business_type,transaction_date,create_date) values" +
                "(?,?,?,?,?,?,?,?,now()) ";

        String updateSql = "update FutureAssetSituation set assets_forecast_inflow = ?," +
                "assets_forecast_outflows = ?," +
                "assets_net_flows = ? , total_inflow = ?,total_outflow = ? ," +
                "total_net_flows = ? where business_type = ? and transaction_date = ?";

        ResultSet rs = null;
        PreparedStatement ps = null;
        //流入计算
        try {

            ps = mysqlCon.prepareStatement(querySql);
            ps.setInt(2,businessType);
            Date transDate = null;
            if(businessType == 3){
                transDate = new Date(simpleDateFormat.parse(borrowingEndDate).getTime());

            }else{
                transDate = new Date(simpleDateFormat.parse(cashierDate).getTime());
            }

            ps.setDate(1,transDate);

            rs = ps.executeQuery();
            BigDecimal assets_forecast_inflow = new BigDecimal(0);
            BigDecimal assets_forecast_outflows = new BigDecimal(0);
            BigDecimal assets_net_flows = new BigDecimal(0);
            BigDecimal total_inflow = new BigDecimal(0);
            BigDecimal total_outflow = new BigDecimal(0);
            BigDecimal total_net_flows = new BigDecimal(0);


            while(rs.next()){
                assets_forecast_inflow = assets_forecast_inflow.add(new BigDecimal(rs.getDouble("assets_forecast_inflow")));
                assets_forecast_outflows = assets_forecast_outflows.add(new BigDecimal(rs.getDouble("assets_forecast_outflows")));
                assets_net_flows = assets_net_flows.add(new BigDecimal(rs.getDouble("assets_net_flows")));
                total_inflow = total_inflow.add(new BigDecimal(rs.getDouble("total_inflow")));
                total_outflow = total_outflow.add(new BigDecimal(rs.getDouble("total_outflow")));
                total_net_flows = total_net_flows.add(new BigDecimal(rs.getDouble("total_net_flows")));
            }


            if(businessType == 3){
                BigDecimal lr =borrowingMoney
                        .multiply(borrowingFate).
                        divide(new BigDecimal(365),10,BigDecimal.ROUND_DOWN).
                        multiply(new BigDecimal(21)).add(borrowingMoney).setScale(3,BigDecimal.ROUND_DOWN);
                if(1 == flowDirection) {
                    assets_forecast_inflow = assets_forecast_inflow.add(lr);
                    assets_net_flows = assets_net_flows.add(lr);
                    total_inflow = total_inflow.add(lr);
                    total_net_flows = total_net_flows.add(lr);
                }else{
                    assets_forecast_inflow = assets_forecast_inflow.subtract(lr);
                    assets_net_flows = assets_net_flows.subtract(lr);
                    total_inflow = total_inflow.subtract(lr);
                    total_net_flows = total_net_flows.subtract(lr);
                }

            }else{
                if(1 == flowDirection) {
                    assets_forecast_inflow = assets_forecast_inflow.add(cashierSum);
                    assets_net_flows = assets_net_flows.add(cashierSum);
                    total_inflow = total_inflow.add(cashierSum);
                    total_net_flows = total_net_flows.add(cashierSum);
                }else{
                    assets_forecast_inflow = assets_forecast_inflow.subtract(cashierSum);
                    assets_net_flows = assets_net_flows.subtract(cashierSum);
                    total_inflow = total_inflow.subtract(cashierSum);
                    total_net_flows = total_net_flows.subtract(cashierSum);
                }
            }

            rs.last();
            if(rs.getRow() == 0){
                ps = mysqlCon.prepareStatement(insertSql);
                ps.setDouble(1,assets_forecast_inflow.doubleValue());
                ps.setDouble(2,assets_forecast_outflows.doubleValue());
//                ps.setDouble(3,assets_asset_residue.doubleValue());
                ps.setDouble(3,assets_net_flows.doubleValue());
                ps.setDouble(4,total_inflow.doubleValue());
                ps.setDouble(5,total_outflow.doubleValue());
                ps.setDouble(6,total_net_flows.doubleValue());
//                ps.setDouble(8,total_asset_residue.doubleValue());
                ps.setInt(7,businessType);
                ps.setDate(8,transDate);
                ps.executeUpdate();
            }else{
                ps = mysqlCon.prepareStatement(updateSql);
                ps.setDouble(1,assets_forecast_inflow.doubleValue());
                ps.setDouble(2,assets_forecast_outflows.doubleValue());
//                ps.setDouble(3,assets_asset_residue.doubleValue());
                ps.setDouble(3,assets_net_flows.doubleValue());
                ps.setDouble(4,total_inflow.doubleValue());
                ps.setDouble(5,total_outflow.doubleValue());
                ps.setDouble(6,total_net_flows.doubleValue());
//                ps.setDouble(8,total_asset_residue.doubleValue());
                ps.setInt(7,businessType);
                ps.setDate(8,transDate);
                ps.executeUpdate();
            }


        //计算流出

            ps = mysqlCon.prepareStatement(querySql);
            ps.setInt(2,businessType);
            transDate = null;
            if(businessType == 3){
                transDate = new Date(simpleDateFormat.parse(borrowingStartDate).getTime());

            }else{
                transDate = new Date(simpleDateFormat.parse(loanDate).getTime());
            }

            ps.setDate(1,transDate);

            rs = ps.executeQuery();

            assets_forecast_inflow = new BigDecimal(0);
            assets_forecast_outflows = new BigDecimal(0);
//            assets_asset_residue = new BigDecimal(0);
            assets_net_flows = new BigDecimal(0);
            total_inflow = new BigDecimal(0);
            total_outflow = new BigDecimal(0);
            total_net_flows = new BigDecimal(0);
//            total_asset_residue = new BigDecimal(0);



            while(rs.next()){
                assets_forecast_inflow = assets_forecast_inflow.add(new BigDecimal(rs.getDouble("assets_forecast_inflow")));
                assets_forecast_outflows = assets_forecast_outflows.add(new BigDecimal(rs.getDouble("assets_forecast_outflows")));
//                assets_asset_residue = assets_asset_residue.add(new BigDecimal(rs.getDouble("assets_asset_residue")));
                assets_net_flows = assets_net_flows.add(new BigDecimal(rs.getDouble("assets_net_flows")));
                total_inflow = total_inflow.add(new BigDecimal(rs.getDouble("total_inflow")));
                total_outflow = total_outflow.add(new BigDecimal(rs.getDouble("total_outflow")));
                total_net_flows = total_net_flows.add(new BigDecimal(rs.getDouble("total_net_flows")));
//                total_asset_residue = total_asset_residue.add(new BigDecimal(rs.getDouble("total_asset_residue")));
            }


            if(businessType == 3){
                if(1 == flowDirection) {
                    assets_forecast_outflows = assets_forecast_outflows.add(borrowingMoney);
                    assets_net_flows = assets_net_flows.subtract(borrowingMoney);
                    total_outflow = total_outflow.add(borrowingMoney);
                    total_net_flows = total_net_flows.subtract(borrowingMoney);
                }else{
                    assets_forecast_outflows = assets_forecast_outflows.subtract(borrowingMoney);
                    assets_net_flows = assets_net_flows.add(borrowingMoney);
                    total_outflow = total_outflow.subtract(borrowingMoney);
                    total_net_flows = total_net_flows.add(borrowingMoney);
                }
            }else{
                if(1 == flowDirection) {
                    assets_forecast_outflows = assets_forecast_outflows.add(loanAmount);
                    assets_net_flows = assets_net_flows.subtract(loanAmount);
                    total_outflow = total_outflow.add(loanAmount);
                    total_net_flows = total_net_flows.subtract(loanAmount);
                }else{
                    assets_forecast_outflows = assets_forecast_outflows.subtract(loanAmount);
                    assets_net_flows = assets_net_flows.add(loanAmount);
                    total_outflow = total_outflow.subtract(loanAmount);
                    total_net_flows = total_net_flows.add(loanAmount);
                }
            }


            rs.last();
            if(rs.getRow() == 0){
                ps = mysqlCon.prepareStatement(insertSql);
                ps.setDouble(1,assets_forecast_inflow.doubleValue());
                ps.setDouble(2,assets_forecast_outflows.doubleValue());
//                ps.setDouble(3,assets_asset_residue.doubleValue());
                ps.setDouble(3,assets_net_flows.doubleValue());
                ps.setDouble(4,total_inflow.doubleValue());
                ps.setDouble(5,total_outflow.doubleValue());
                ps.setDouble(6,total_net_flows.doubleValue());
//                ps.setDouble(8,total_asset_residue.doubleValue());
                ps.setInt(7,businessType);
                ps.setDate(8,transDate);
                ps.executeUpdate();
            }else{
                ps = mysqlCon.prepareStatement(updateSql);
                ps.setDouble(1,assets_forecast_inflow.doubleValue());
                ps.setDouble(2,assets_forecast_outflows.doubleValue());
//                ps.setDouble(3,assets_asset_residue.doubleValue());
                ps.setDouble(3,assets_net_flows.doubleValue());
                ps.setDouble(4,total_inflow.doubleValue());
                ps.setDouble(5,total_outflow.doubleValue());
                ps.setDouble(6,total_net_flows.doubleValue());
//                ps.setDouble(8,total_asset_residue.doubleValue());
                ps.setInt(7,businessType);
                ps.setDate(8,transDate);
                ps.executeUpdate();
            }



            String updateAssetResidue = "update  FutureAssetSituation f1 " +
                    " set assets_asset_residue =  " +
                    " (" +
                    "   select sum(tmp.a)  from  " +
                    " (   select sum(total_net_flows) a,transaction_date " +
                    "       from FutureAssetSituation f2   where business_type = ?       " +
                    " group by transaction_date  " +
                    " ) as tmp " +
                    " where  transaction_date <= f1.transaction_date " +
                    " ),total_asset_residue = assets_asset_residue" +
                    " where business_type = ?";
            ps = mysqlCon.prepareStatement(updateAssetResidue);
            ps.setDouble(1,businessType);
            ps.setDouble(2,businessType);

            ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

        } catch (ParseException e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }


    }

    public void businessAdd2FutureAssetSituation(List<CanalEntry.Column> columnList,Connection mysqlCon,int flowDirection){

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        simpleDateFormat.setLenient(false);

        BigDecimal contractAmount = null;
        String collectionDays = null;

        BigDecimal collectionAmount = null;
        BigDecimal amountOfDefault = null;


        //流出
        BigDecimal accountAmount = null;
        String accountDay = null;

        BigDecimal accountPrincipal = null;
        BigDecimal accountInterest = null;

        int orderType = 0;

        for (CanalEntry.Column column : columnList) {
            if("orderType".equals(column.getName())){
                orderType = Integer.parseInt(column.getValue());
            }

            if("collectionDays".equals(column.getName())){
                collectionDays = column.getValue();
            }
            if("accountDay".equals(column.getName())){
                accountDay = column.getValue();
            }
            if("contractAmount".equals(column.getName())){
                contractAmount = new BigDecimal(column.getValue().isEmpty()?"0":column.getValue());
            }
            if("collectionAmount".equals(column.getName())){
                collectionAmount = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }
            if("amountOfDefault".equals(column.getName())){
                amountOfDefault = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }
            if("accountAmount".equals(column.getName())){
                accountAmount = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }
            if("accountPrincipal".equals(column.getName())){
                accountPrincipal = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }
            if("accountInterest".equals(column.getName())){
                accountInterest = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }

        }

        if(orderType != 3 && orderType != 4){
            return;
        }


        String querySql = "select business_forecast_inflow,business_forecast_outflows,business_asset_residue,business_net_flows,total_inflow,total_outflow,total_net_flows,total_asset_residue" +
                " from FutureAssetSituation where transaction_date = ? and business_type = 5";

        String insertSql = "insert into FutureAssetSituation(business_forecast_inflow,business_forecast_outflows," +
                "business_net_flows,total_inflow,total_outflow," +
                "total_net_flows,business_type,transaction_date,create_date) values" +
                "(?,?,?,?,?,?,5,?,now()) ";

        String updateSql = "update FutureAssetSituation set business_forecast_inflow = ?," +
                "business_forecast_outflows = ?," +
                "business_net_flows = ? , total_inflow = ?,total_outflow = ? ," +
                "total_net_flows = ? where business_type = 5 and transaction_date = ?";

        ResultSet rs = null;
        PreparedStatement ps = null;
        //流入计算
        try {

            ps = mysqlCon.prepareStatement(querySql);
            ps.setDate(1,new Date(simpleDateFormat.parse(collectionDays).getTime()));

            rs = ps.executeQuery();
            BigDecimal business_forecast_inflow = new BigDecimal(0);
            BigDecimal business_forecast_outflows = new BigDecimal(0);
            BigDecimal business_net_flows = new BigDecimal(0);
            BigDecimal total_inflow = new BigDecimal(0);
            BigDecimal total_outflow = new BigDecimal(0);
            BigDecimal total_net_flows = new BigDecimal(0);


            while(rs.next()){
                business_forecast_inflow = business_forecast_inflow.add(new BigDecimal(rs.getDouble("business_forecast_inflow")));
                business_forecast_outflows = business_forecast_outflows.add(new BigDecimal(rs.getDouble("business_forecast_outflows")));
                business_net_flows = business_net_flows.add(new BigDecimal(rs.getDouble("business_net_flows")));
                total_inflow = total_inflow.add(new BigDecimal(rs.getDouble("total_inflow")));
                total_outflow = total_outflow.add(new BigDecimal(rs.getDouble("total_outflow")));
                total_net_flows = total_net_flows.add(new BigDecimal(rs.getDouble("total_net_flows")));

            }


            if(orderType == 3){
                if( 1 == flowDirection) {
                    business_forecast_inflow = business_forecast_inflow.add(contractAmount);
                    business_net_flows = business_net_flows.add(contractAmount);
                    total_inflow = total_inflow.add(contractAmount);
                    total_net_flows = total_net_flows.add(contractAmount);
                }else{
                    business_forecast_inflow = business_forecast_inflow.subtract(contractAmount);
                    business_net_flows = business_net_flows.subtract(contractAmount);
                    total_inflow = total_inflow.subtract(contractAmount);
                    total_net_flows = total_net_flows.subtract(contractAmount);
                }
            }else{
                if(1 == flowDirection) {
                    business_forecast_inflow = business_forecast_inflow.add(collectionAmount.add(amountOfDefault));
                    business_net_flows = business_net_flows.add(collectionAmount.add(amountOfDefault));
                    total_inflow = total_inflow.add(collectionAmount.add(amountOfDefault));
                    total_net_flows = total_net_flows.add(collectionAmount.add(amountOfDefault));
                }else{
                    business_forecast_inflow = business_forecast_inflow.subtract(collectionAmount.add(amountOfDefault));
                    business_net_flows = business_net_flows.subtract(collectionAmount.add(amountOfDefault));
                    total_inflow = total_inflow.subtract(collectionAmount.add(amountOfDefault));
                    total_net_flows = total_net_flows.subtract(collectionAmount.add(amountOfDefault));
                }
            }


            rs.last();
            if(rs.getRow() == 0){
                ps = mysqlCon.prepareStatement(insertSql);
                ps.setDouble(1,business_forecast_inflow.doubleValue());
                ps.setDouble(2,business_forecast_outflows.doubleValue());
                ps.setDouble(3,business_net_flows.doubleValue());
                ps.setDouble(4,total_inflow.doubleValue());
                ps.setDouble(5,total_outflow.doubleValue());
                ps.setDouble(6,total_net_flows.doubleValue());
                ps.setDate(7,new Date(simpleDateFormat.parse(collectionDays).getTime()));
                ps.executeUpdate();
            }else{
                ps = mysqlCon.prepareStatement(updateSql);
                ps.setDouble(1,business_forecast_inflow.doubleValue());
                ps.setDouble(2,business_forecast_outflows.doubleValue());
                ps.setDouble(3,business_net_flows.doubleValue());
                ps.setDouble(4,total_inflow.doubleValue());
                ps.setDouble(5,total_outflow.doubleValue());
                ps.setDouble(6,total_net_flows.doubleValue());
                ps.setDate(7,new Date(simpleDateFormat.parse(collectionDays).getTime()));
                ps.executeUpdate();
            }


            //计算流出

            ps = mysqlCon.prepareStatement(querySql);
            ps.setDate(1,new Date(simpleDateFormat.parse(accountDay).getTime()));

            rs = ps.executeQuery();

            business_forecast_inflow = new BigDecimal(0);
            business_forecast_outflows = new BigDecimal(0);
            business_net_flows = new BigDecimal(0);
            total_inflow = new BigDecimal(0);
            total_outflow = new BigDecimal(0);
            total_net_flows = new BigDecimal(0);



            while(rs.next()){
                business_forecast_inflow = business_forecast_inflow.add(new BigDecimal(rs.getDouble("business_forecast_inflow")));
                business_forecast_outflows = business_forecast_outflows.add(new BigDecimal(rs.getDouble("business_forecast_outflows")));
                business_net_flows = business_net_flows.add(new BigDecimal(rs.getDouble("business_net_flows")));
                total_inflow = total_inflow.add(new BigDecimal(rs.getDouble("total_inflow")));
                total_outflow = total_outflow.add(new BigDecimal(rs.getDouble("total_outflow")));
                total_net_flows = total_net_flows.add(new BigDecimal(rs.getDouble("total_net_flows")));
            }


            if(orderType == 3){
                if(1 == flowDirection) {
                    business_forecast_outflows = business_forecast_outflows.add(accountAmount);
                    business_net_flows = business_net_flows.subtract(accountAmount);
                    total_outflow = total_outflow.add(accountAmount);
                    total_net_flows = total_net_flows.subtract(accountAmount);
                }else{
                    business_forecast_outflows = business_forecast_outflows.subtract(accountAmount);
                    business_net_flows = business_net_flows.add(accountAmount);
                    total_outflow = total_outflow.subtract(accountAmount);
                    total_net_flows = total_net_flows.add(accountAmount);
                }
            }else{
                if(1== flowDirection) {
                    business_forecast_outflows = business_forecast_outflows.add(accountPrincipal.add(accountInterest));
                    business_net_flows = business_net_flows.subtract(accountPrincipal.add(accountInterest));
                    total_outflow = total_outflow.add(accountPrincipal.add(accountInterest));
                    total_net_flows = total_net_flows.subtract(accountPrincipal.add(accountInterest));
                }else{
                    business_forecast_outflows = business_forecast_outflows.subtract(accountPrincipal.add(accountInterest));
                    business_net_flows = business_net_flows.add(accountPrincipal.add(accountInterest));
                    total_outflow = total_outflow.subtract(accountPrincipal.add(accountInterest));
                    total_net_flows = total_net_flows.add(accountPrincipal.add(accountInterest));
                }
            }


            rs.last();
            if(rs.getRow() == 0){
                ps = mysqlCon.prepareStatement(insertSql);
                ps.setDouble(1,business_forecast_inflow.doubleValue());
                ps.setDouble(2,business_forecast_outflows.doubleValue());
                ps.setDouble(3,business_net_flows.doubleValue());
                ps.setDouble(4,total_inflow.doubleValue());
                ps.setDouble(5,total_outflow.doubleValue());
                ps.setDouble(6,total_net_flows.doubleValue());
                ps.setDate(7,new Date(simpleDateFormat.parse(accountDay).getTime()));
                ps.executeUpdate();
            }else{
                ps = mysqlCon.prepareStatement(updateSql);
                ps.setDouble(1,business_forecast_inflow.doubleValue());
                ps.setDouble(2,business_forecast_outflows.doubleValue());
                ps.setDouble(3,business_net_flows.doubleValue());
                ps.setDouble(4,total_inflow.doubleValue());
                ps.setDouble(5,total_outflow.doubleValue());
                ps.setDouble(6,total_net_flows.doubleValue());
                ps.setDate(7,new Date(simpleDateFormat.parse(accountDay).getTime()));
                ps.executeUpdate();
            }




            String updateAssetResidue = " update  FutureAssetSituation f1 " +
                    " set business_asset_residue =  " +
                    " (" +
                    " select sum(tmp.a)  from  " +
                    " (   select sum(total_net_flows) a,transaction_date " +
                    " from FutureAssetSituation f2   where business_type = ?       " +
                    " group by transaction_date  " +
                    " ) as tmp " +
                    " where  transaction_date <= f1.transaction_date " +
                    " ),total_asset_residue = business_asset_residue" +
                    " where business_type = ?";

            ps = mysqlCon.prepareStatement(updateAssetResidue);
            ps.setDouble(1,5);
            ps.setDouble(2,5);

            ps.executeUpdate();


        } catch (SQLException e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

        } catch (ParseException e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }

    }

    public void assetAdd2FutureAssetSituationForecast(List<CanalEntry.Column> columnList,Connection mysqlCon,int businessType,int flowDirection){

        String borrowingEndDate = "";
        String cashierDate = "";
        String borrowingStartDate = "";
        String loanDate = "";

        BigDecimal loanAmount = null;
        BigDecimal borrowingMoney = null;
        BigDecimal borrowingFate = null;
        BigDecimal cashierSum = null;

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        simpleDateFormat.setLenient(false);

        for (CanalEntry.Column column : columnList) {
            if("borrowingEndDate".equals(column.getName())){
                borrowingEndDate = column.getValue();
            }
            if("borrowingMoney".equals(column.getName())){
                borrowingMoney = new BigDecimal(column.getValue());
            }
            if("borrowingFate".equals(column.getName())){
                borrowingFate = new BigDecimal(column.getValue());
            }
            if("cashierDate".equals(column.getName())){
                cashierDate = column.getValue();
            }
            if("cashierSum".equals(column.getName())){
                cashierSum = new BigDecimal(column.getValue());
            }
            if("borrowingStartDate".equals(column.getName())){
                borrowingStartDate = column.getValue();
            }
            if("loanDate".equals(column.getName())){
                loanDate = column.getValue();
            }
            if("loanAmount".equals(column.getName())){
                loanAmount = new BigDecimal(column.getValue());
            }

        }

        String querySql = "select assets_forecast_inflow,assets_forecast_outflows,assets_asset_residue" +
                " from FutureAssetSituationForecast where forecast_date = ? and business_type = ?";

        String insertSql = "insert into FutureAssetSituationForecast(assets_forecast_inflow,assets_forecast_outflows," +
                "business_type,forecast_date,create_date) values" +
                "(?,?,?,?,now()) ";

        String updateSql = "update FutureAssetSituationForecast set assets_forecast_inflow = ?," +
                "assets_forecast_outflows = ? " +
                " where business_type = ? and forecast_date = ?";

        ResultSet rs = null;
        PreparedStatement ps = null;
        //流入计算
        try {

            ps = mysqlCon.prepareStatement(querySql);
            ps.setInt(2,businessType);
            Date transDate = null;
            if(businessType == 3){
                transDate = new Date(simpleDateFormat.parse(borrowingEndDate).getTime());

            }else{
                transDate = new Date(simpleDateFormat.parse(cashierDate).getTime());
            }

            ps.setDate(1,transDate);

            rs = ps.executeQuery();
            BigDecimal assets_forecast_inflow = new BigDecimal(0);
            BigDecimal assets_forecast_outflows = new BigDecimal(0);



            while(rs.next()){
                assets_forecast_inflow = assets_forecast_inflow.add(new BigDecimal(rs.getDouble("assets_forecast_inflow")));
                assets_forecast_outflows = assets_forecast_outflows.add(new BigDecimal(rs.getDouble("assets_forecast_outflows")));
            }


            if(businessType == 3){
                BigDecimal lr =borrowingMoney
                        .multiply(borrowingFate).
                                divide(new BigDecimal(365),10,BigDecimal.ROUND_DOWN).
                                multiply(new BigDecimal(21)).add(borrowingMoney).setScale(3,BigDecimal.ROUND_DOWN);
                if(1 == flowDirection) {
                    assets_forecast_inflow = assets_forecast_inflow.add(lr);
                }else{
                    assets_forecast_inflow = assets_forecast_inflow.subtract(lr);
                }
            }else{
                if(1 == flowDirection) {
                    assets_forecast_inflow = assets_forecast_inflow.add(cashierSum);
                }else{
                    assets_forecast_inflow = assets_forecast_inflow.subtract(cashierSum);
                }
            }

            rs.last();
            if(rs.getRow() == 0){
                ps = mysqlCon.prepareStatement(insertSql);
                ps.setDouble(1,assets_forecast_inflow.doubleValue());
                ps.setDouble(2,assets_forecast_outflows.doubleValue());
                ps.setDouble(3,businessType);
                ps.setDate(4,transDate);
                ps.executeUpdate();
            }else{
                ps = mysqlCon.prepareStatement(updateSql);
                ps.setDouble(1,assets_forecast_inflow.doubleValue());
                ps.setDouble(2,assets_forecast_outflows.doubleValue());
                ps.setDouble(3,businessType);
                ps.setDate(4,transDate);
                ps.executeUpdate();
            }


            //计算流出

            ps = mysqlCon.prepareStatement(querySql);
            ps.setInt(2,businessType);
            transDate = null;
            if(businessType == 3){
                transDate = new Date(simpleDateFormat.parse(borrowingStartDate).getTime());

            }else{
                transDate = new Date(simpleDateFormat.parse(loanDate).getTime());
            }

            ps.setDate(1,transDate);

            rs = ps.executeQuery();

            assets_forecast_inflow = new BigDecimal(0);
            assets_forecast_outflows = new BigDecimal(0);



            while(rs.next()){
                assets_forecast_inflow = assets_forecast_inflow.add(new BigDecimal(rs.getDouble("assets_forecast_inflow")));
                assets_forecast_outflows = assets_forecast_outflows.add(new BigDecimal(rs.getDouble("assets_forecast_outflows")));;
            }


            if(businessType == 3){
                if(1 == flowDirection) {
                    assets_forecast_outflows = assets_forecast_outflows.add(borrowingMoney);
                }else{
                    assets_forecast_outflows = assets_forecast_outflows.subtract(borrowingMoney);
                }
            }else{
                if(1== flowDirection) {
                    assets_forecast_outflows = assets_forecast_outflows.add(loanAmount);
                }else{
                    assets_forecast_outflows = assets_forecast_outflows.subtract(loanAmount);
                }
            }


            rs.last();
            if(rs.getRow() == 0){
                ps = mysqlCon.prepareStatement(insertSql);
                ps.setDouble(1,assets_forecast_inflow.doubleValue());
                ps.setDouble(2,assets_forecast_outflows.doubleValue());
                ps.setInt(3,businessType);
                ps.setDate(4,transDate);
                ps.executeUpdate();
            }else{
                ps = mysqlCon.prepareStatement(updateSql);
                ps.setDouble(1,assets_forecast_inflow.doubleValue());
                ps.setDouble(2,assets_forecast_outflows.doubleValue());
                ps.setInt(3,businessType);
                ps.setDate(4,transDate);
                ps.executeUpdate();
            }



            String updateAssetResidue = "update  FutureAssetSituationForecast f1 " +
                    " set assets_asset_residue =  " +
                    " (" +
                    "   select sum(tmp.a)  from  " +
                    " (   select sum(assets_forecast_inflow - assets_forecast_outflows) a,forecast_date " +
                    "       from FutureAssetSituationForecast f2   where business_type = ?       " +
                    " group by forecast_date  " +
                    " ) as tmp " +
                    " where  forecast_date <= f1.forecast_date " +
                    " )" +
                    " where business_type = ?";
            ps = mysqlCon.prepareStatement(updateAssetResidue);
            ps.setDouble(1,businessType);
            ps.setDouble(2,businessType);

            ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

        } catch (ParseException e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }


    }

    public void businessAdd2FutureAssetSituationForecast(List<CanalEntry.Column> columnList,Connection mysqlCon,int flowDirection){

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        simpleDateFormat.setLenient(false);

        BigDecimal contractAmount = null;
        String collectionDays = null;

        BigDecimal collectionAmount = null;
        BigDecimal amountOfDefault = null;


        //流出
        BigDecimal accountAmount = null;
        String accountDay = null;

        BigDecimal accountPrincipal = null;
        BigDecimal accountInterest = null;

        int orderType = 0;

        for (CanalEntry.Column column : columnList) {
            if("orderType".equals(column.getName())){
                orderType = Integer.parseInt(column.getValue());
            }

            if("collectionDays".equals(column.getName())){
                collectionDays = column.getValue();
            }
            if("accountDay".equals(column.getName())){
                accountDay = column.getValue();
            }
            if("contractAmount".equals(column.getName())){
                contractAmount = new BigDecimal(column.getValue().isEmpty()?"0":column.getValue());
            }
            if("collectionAmount".equals(column.getName())){
                collectionAmount = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }
            if("amountOfDefault".equals(column.getName())){
                amountOfDefault = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }
            if("accountAmount".equals(column.getName())){
                accountAmount = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }
            if("accountPrincipal".equals(column.getName())){
                accountPrincipal = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }
            if("accountInterest".equals(column.getName())){
                accountInterest = new BigDecimal(column.getValue().isEmpty() ?"0":column.getValue());
            }

        }

        if(orderType != 3 && orderType != 4){
            return;
        }


        String querySql = "select business_forecast_inflow,business_forecast_outflows,business_asset_residue" +
                " from FutureAssetSituationForecast where forecast_date = ? and business_type = 5";

        String insertSql = "insert into FutureAssetSituationForecast(business_forecast_inflow,business_forecast_outflows," +
                " business_type,forecast_date,create_date) values" +
                "(?,?,5,?,now()) ";

        String updateSql = "update FutureAssetSituationForecast set business_forecast_inflow = ?," +
                "business_forecast_outflows = ? where business_type = 5 and forecast_date = ?";

        ResultSet rs = null;
        PreparedStatement ps = null;
        //流入计算
        try {

            ps = mysqlCon.prepareStatement(querySql);
            ps.setDate(1,new Date(simpleDateFormat.parse(collectionDays).getTime()));

            rs = ps.executeQuery();
            BigDecimal business_forecast_inflow = new BigDecimal(0);
            BigDecimal business_forecast_outflows = new BigDecimal(0);


            while(rs.next()){
                business_forecast_inflow = business_forecast_inflow.add(new BigDecimal(rs.getDouble("business_forecast_inflow")));
                business_forecast_outflows = business_forecast_outflows.add(new BigDecimal(rs.getDouble("business_forecast_outflows")));

            }


            if(orderType == 3){
                if(1 == flowDirection) {
                    business_forecast_inflow = business_forecast_inflow.add(contractAmount);
                }else{
                    business_forecast_inflow = business_forecast_inflow.subtract(contractAmount);
                }
            }else{
                if(1 == flowDirection) {
                    business_forecast_inflow = business_forecast_inflow.add(collectionAmount.add(amountOfDefault));
                }else{
                    business_forecast_inflow = business_forecast_inflow.subtract(collectionAmount.add(amountOfDefault));
                }
            }


            rs.last();
            if(rs.getRow() == 0){
                ps = mysqlCon.prepareStatement(insertSql);
                ps.setDouble(1,business_forecast_inflow.doubleValue());
                ps.setDouble(2,business_forecast_outflows.doubleValue());
                ps.setDate(3,new Date(simpleDateFormat.parse(collectionDays).getTime()));
                ps.executeUpdate();
            }else{
                ps = mysqlCon.prepareStatement(updateSql);
                ps.setDouble(1,business_forecast_inflow.doubleValue());
                ps.setDouble(2,business_forecast_outflows.doubleValue());
                ps.setDate(3,new Date(simpleDateFormat.parse(collectionDays).getTime()));
                ps.executeUpdate();
            }


            //计算流出

            ps = mysqlCon.prepareStatement(querySql);
            ps.setDate(1,new Date(simpleDateFormat.parse(accountDay).getTime()));

            rs = ps.executeQuery();

            business_forecast_inflow = new BigDecimal(0);
            business_forecast_outflows = new BigDecimal(0);



            while(rs.next()){
                business_forecast_inflow = business_forecast_inflow.add(new BigDecimal(rs.getDouble("business_forecast_inflow")));
                business_forecast_outflows = business_forecast_outflows.add(new BigDecimal(rs.getDouble("business_forecast_outflows")));
            }


            if(orderType == 3){
                if(1 == flowDirection) {
                    business_forecast_outflows = business_forecast_outflows.add(accountAmount);
                }else{
                    business_forecast_outflows = business_forecast_outflows.subtract(accountAmount);
                }
            }else{
                if(1 == flowDirection) {
                    business_forecast_outflows = business_forecast_outflows.add(accountPrincipal.add(accountInterest));
                }else{
                    business_forecast_outflows = business_forecast_outflows.subtract(accountPrincipal.add(accountInterest));
                }
            }


            rs.last();
            if(rs.getRow() == 0){
                ps = mysqlCon.prepareStatement(insertSql);
                ps.setDouble(1,business_forecast_inflow.doubleValue());
                ps.setDouble(2,business_forecast_outflows.doubleValue());
                ps.setDate(3,new Date(simpleDateFormat.parse(accountDay).getTime()));
                ps.executeUpdate();
            }else{
                ps = mysqlCon.prepareStatement(updateSql);
                ps.setDouble(1,business_forecast_inflow.doubleValue());
                ps.setDouble(2,business_forecast_outflows.doubleValue());
                ps.setDate(3,new Date(simpleDateFormat.parse(accountDay).getTime()));
                ps.executeUpdate();
            }




            String updateAssetResidue = " update  FutureAssetSituationForecast f1 " +
                    " set business_asset_residue =  " +
                    " (" +
                    " select sum(tmp.a)  from  " +
                    " (   select sum(business_forecast_inflow - business_forecast_outflows) a,forecast_date " +
                    " from FutureAssetSituationForecast f2   where business_type = ?       " +
                    " group by forecast_date  " +
                    " ) as tmp " +
                    " where  forecast_date <= f1.forecast_date " +
                    " ) " +
                    " where business_type = ?";

            ps = mysqlCon.prepareStatement(updateAssetResidue);
            ps.setDouble(1,5);
            ps.setDouble(2,5);

            ps.executeUpdate();


        } catch (SQLException e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

        } catch (ParseException e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }

    }



    public void assetUpdate2FutureAssetSituation(List<CanalEntry.Column> columnList,List<CanalEntry.Column> before,Connection mysqlCon,int businessType){
        assetAdd2FutureAssetSituation(before,mysqlCon,businessType,-1);
        assetAdd2FutureAssetSituation(columnList,mysqlCon,businessType,1);

    }

    public void businessUpdate2FutureAssetSituation(List<CanalEntry.Column> columnList,List<CanalEntry.Column> before,Connection mysqlCon){
        businessAdd2FutureAssetSituation(before,mysqlCon,-1);
        businessAdd2FutureAssetSituation(columnList,mysqlCon,1);

    }

    public void assetUpdate2FutureAssetSituationForecast(List<CanalEntry.Column> columnList,List<CanalEntry.Column> before,Connection mysqlCon,int businessType){
        assetAdd2FutureAssetSituationForecast(before,mysqlCon,businessType,-1);
        assetAdd2FutureAssetSituationForecast(columnList,mysqlCon,businessType,1);


    }

    public void businessUpdate2FutureAssetSituationForecast(List<CanalEntry.Column> columnList,List<CanalEntry.Column> before,Connection mysqlCon){
        businessAdd2FutureAssetSituationForecast(before,mysqlCon,-1);
        businessAdd2FutureAssetSituationForecast(columnList,mysqlCon,1);
    }



}
