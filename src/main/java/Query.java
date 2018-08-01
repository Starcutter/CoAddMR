public class Query {
    double[] query = new double[4];
    char band;
    int colMin, colMax;

    public Query(double[] query, char band) {
        for (int i = 0; i < 4; i++) {
            this.query[i] = query[i];
        }
        this.band = band;
        if (this.query[2] > 1.35) {
            colMin = 7;
        } else if (this.query[2] > 0.90) {
            colMin = 6;
        } else if (this.query[2] > 0.50) {
            colMin = 5;
        } else if (this.query[2] > 0.05) {
            colMin = 4;
        } else if (this.query[2] > -0.35) {
            colMin = 3;
        } else if (this.query[2] > -0.75) {
            colMin = 2;
        } else {
            colMin = 1;
        }
        if (this.query[3] < -1.35) {
            colMax = 0;
        } else if (this.query[3] < -0.90) {
            colMax = 1;
        } else if (this.query[3] < -0.50) {
            colMax = 2;
        } else if (this.query[3] < -0.05) {
            colMax = 3;
        } else if (this.query[3] < 0.35) {
            colMax = 4;
        } else if (this.query[3] < 0.80) {
            colMax = 5;
        } else {
            colMax = 6;
        }
    }

    public boolean isOverlapping(int camcol, char band) {
        return (this.colMin <= camcol && camcol <= this.colMax && this.band == band);
    }

    public static Query[] getQueries(String queryStr) {
        /*
        raMin1,raMax1,decMin1,decMax1,band1;raMin2,raMax2,decMin2,decMax2,band2; ...
         */
        String[] strQueries = queryStr.split(";");
        Query[] queries = new Query[strQueries.length];
        for (int i = 0; i < strQueries.length; i++) {
            String[] splits = strQueries[i].split(",");
            double[] q = new double[4];
            for (int j = 0; j < 4; j++) {
                q[j] = Double.parseDouble(splits[j]);
            }
            queries[i] = new Query(q, splits[4].charAt(0));
        }
        return queries;
    }
}
