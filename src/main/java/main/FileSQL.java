package main;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;

public class FileSQL {
    private BufferedReader brFileDump;
    private String queryAllInsertNonOrdinate;
    private List<String> listaFinalQueries = new LinkedList<>();
    private LinkedHashSet<String> listaIdInsertFileTabIssues = new LinkedHashSet<>();
    private LinkedHashSet<String> listaDataCreazioneBugInsertFileTabIssues = new LinkedHashSet<>();
    private LinkedHashSet<String> listaIDInsertSplittedTabIssues = new LinkedHashSet<>();
    private LinkedHashSet<String> listaDataCreazioneBugInsertSplittedTabIssues = new LinkedHashSet<>();
    private String nomeTabella;

    public FileSQL(String nomeFile) throws IOException {
        this.brFileDump = new BufferedReader(new FileReader("src/main/resources/files/"+nomeFile));
    }
    public void close() throws IOException {
        this.brFileDump.close();
    }
    public void creaDumpCorretto() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/resources/files/prova_dump_nuovo.sql"));
        String line;
        while((line = brFileDump.readLine()) != null){
            if(line.startsWith("INSERT")){
                List<String> listaValuesRigheInsertFile = splitStringaQueryAllInsertFile(line);
                List<String> listaValuesRigheInsertSplitted = splitListaRigheInsertQueryOrdinate();
                if(checkQueryValuesFromDumpEqualsQueryValuesSplittate(listaValuesRigheInsertFile,listaValuesRigheInsertSplitted)){
                    listaFinalQueries.forEach(query -> {
                        try {
                            writer.write(query);
                            writer.newLine();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }else{
                    System.err.println("La Lista di valori splittata per limite di 30 Inserts è diverso dalla Insert del File DUmp");
                }
            }else{
                writer.write(line);
                writer.newLine();
            }
            listaFinalQueries.clear();
            nomeTabella="";
            writer.flush();
        }
    }

    /**Metodo che ottiene la stringa con la query insert per la tabella 'issues'
     * la stringa si trova alla riga 858 del file .sql*/
    public void getAllInsertQueriesFileNonSplittateTabIssues() throws IOException {
        String insertQuery858 = "";
        int lineNumber = 0;
        String line;
        while ((line = brFileDump.readLine()) != null) {
            lineNumber++;
            if (lineNumber == 858 && line.startsWith("INSERT")) {
                insertQuery858 = line;
                break;
            }
        }
        this.queryAllInsertNonOrdinate = insertQuery858;
    }

    /**Metodo per ottenere un array di campi della query con tutti gli insert nella stessa riga.
     //L'array verrà usato per ottenere la lista di id e date delle insert per poterle controllare
     //  con la lista di id e date ottenute dalle insert query ordinate per un limite di 30 insert
     @param queryAll stringa contenente la query con gli insert dal file sql
     @return Lista dei campi delle righe insert  */
    public List<String> splitStringaQueryAllInsertFile(String queryAll) throws IOException {

        // List to store final queries
        String vvv = queryAll.substring(0, queryAll.length()-2);
        String[]  arrayInsertsSubstring = vvv.split("VALUES \\(");
        if(arrayInsertsSubstring.length>1){
            nomeTabella = arrayInsertsSubstring[0];
            String valoriInsertsSenzaInsertIntoValues = arrayInsertsSubstring[1].trim();
            String[] inserts = valoriInsertsSenzaInsertIntoValues.split("\\),\\(");
            scorriArrayInserimentiPopolaListaInsert(inserts);
            return(Arrays.asList(inserts));
        }
        //String valuesPart = vvv.split("VALUES\\(")[1].trim();
        //String[] inserts = valuesPart.split("\\),\\(");
        return Arrays.asList("");
    }

    /**Metodo per dividere la lista di query ordinate per un limite di 30 insert per query.
     Viene creato un array per ogni query della lista
     L'array verrà usato per potere ottenere i campi
     dai campi si otterrà la lista di id e date da confrontare con quelle del file originale
     @return Lista dei campi delle righe insert*/
    public List<String> splitListaRigheInsertQueryOrdinate(){

        List<String> listaBuffer = new LinkedList<>();
        int n = 0;
        for(String query : getListaFinalQueries()){
            String vvv = query.substring(0, query.length()-2);
            String[]  arrayInsertsSubstring = vvv.split("VALUES \\(");
            if(arrayInsertsSubstring.length>1){
                String valoriInsertsSenzaInsertIntoValues = arrayInsertsSubstring[1].trim();
                String[] inserts = valoriInsertsSenzaInsertIntoValues.split("\\),\\(");
                listaBuffer.addAll(Arrays.asList(inserts));
                n++;
            }
        }
        getListaFinalQueries().forEach(query->{
        });
        return listaBuffer;
    }

    /**Scorre l'array della query insert
     * */
    private void scorriArrayInserimentiPopolaListaInsert(String[] inserts){
        StringBuilder listaProvvQuery= new StringBuilder();
        //contatore di insert nell'array degli insert
        int numInsert=0;
        for(int i = 0; i<inserts.length; i++) {
            numInsert++;
            if(numInsert<=30&& i!=inserts.length-1){
                aggiungiInsertTabIssues(inserts[i],listaProvvQuery);
            }else if(numInsert>30){
                numInsert=0;
                i--;
                terminaRigaInsertTabIssues(listaProvvQuery);
            }else{
                listaProvvQuery.append("(").append(inserts[i]);
                terminaUltimaRigaInsertTabIssues(listaProvvQuery);
            }
        }
    }
    private void aggiungiInsertTabIssues(String insert, StringBuilder sb ){
        sb.append("(");
        sb.append(insert).append("),");
    }
    private void terminaUltimaRigaInsertTabIssues(StringBuilder sb){
        sb.append(");");

        sb.insert(0, nomeTabella+"VALUES ");
        listaFinalQueries.add(sb.toString());
        sb.setLength(0);
    }
    private void terminaRigaInsertTabIssues(StringBuilder sb){
        sb.deleteCharAt(sb.length()-1);
        sb.append(";");
        sb.insert(0, nomeTabella+"VALUES ");
        listaFinalQueries.add(sb.toString());
        sb.setLength(0);
    }
    public void scriviListaFinalQueriesTabIssuesInFile() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/resources/files/script_insert_into_issues.sql"));
        listaFinalQueries.forEach(e->{
            try {
                writer.write(e.toString());
                writer.newLine();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
        writer.flush();
        writer.close();
    }
    public void stampaQueriesFinali(List<String> listaFinalQueriesTabIssues){
        listaFinalQueriesTabIssues.forEach(e->{
            System.out.println(e.toString());
        });
    }
    private void popolaListeIdDataCreazioneBugInsertOrdinateTabIssues(String[] inserts) throws IOException {
        for(String input : inserts){
            // Create a custom CSVFormat
            CSVFormat csvFormat = CSVFormat.Builder.create()
                    .setDelimiter(',')
                    .setQuote('\'')
                    .setEscape('\\')
                    .build();

            // Use the custom CSVFormat to parse the inputString
            CSVParser csvParser = new CSVParser(new StringReader(input), csvFormat);
            for (CSVRecord csvRecord : csvParser) {
                for (int i = 0; i < csvRecord.size(); i++) {
                    String field = csvRecord.get(i);

                    // Check if the field is enclosed in single quotes
                    if (field.startsWith("'") && field.endsWith("'")) {
                        // Remove the single quotes
                        field = field.substring(1, field.length() - 1);
                    }
                    if(i==0){
                        listaIDInsertSplittedTabIssues.add(field.trim());
                    }else if(i==13){
                        listaDataCreazioneBugInsertSplittedTabIssues.add(field.trim());
                    }
                }
            }
        }
    }
    public void popolaListeIdDataCreazioneBugInsertNonOrdinateTabIssues(String[] inserts) throws IOException {
        /*String queryAll = getQueryAllInsertTabIssuesNonOrdinate();
        String vvv = queryAll.substring(0, queryAll.length()-2);
        String[]  arrayInsertsSubstring = vvv.split("VALUES \\(");
        String valoriInsertsSenzaInsertIntoValues = arrayInsertsSubstring[1].trim();
        String[] inserts = valoriInsertsSenzaInsertIntoValues.split("\\),\\(");

        for(String s : inserts){
            CSVParser csvParser = CSVParser.parse(s, CSVFormat.DEFAULT);
            for (CSVRecord csvRecord : csvParser) {
                for (int i = 0; i<csvRecord.values().length;i++) {
                    if(i==13){
                        System.out.println(csvRecord.get(i));
                    }
                }
            }
        }*/
        //String inputString = "1,2,5,'Contratti','Rivisitazione della pagina Contratti aggiunta della gestione multipla dei canoni, gestione della data di sgancio dell\\'unità immobiliare.',NULL,NULL,5,9,3,NULL,6,2,'2017-02-06 09:54:26','2017-05-19 12:31:28','2017-02-01',100,80,NULL,1,1,2,0,'2017-05-19 12:31:28'";
        for(String input : inserts){
            // Create a custom CSVFormat
            CSVFormat csvFormat = CSVFormat.Builder.create()
                    .setDelimiter(',')
                    .setQuote('\'')
                    .setEscape('\\')
                    .build();

            // Use the custom CSVFormat to parse the inputString
            CSVParser csvParser = new CSVParser(new StringReader(input), csvFormat);
            for (CSVRecord csvRecord : csvParser) {
                for (int i = 0; i < csvRecord.size(); i++) {
                    String field = csvRecord.get(i);

                    // Check if the field is enclosed in single quotes
                    if (field.startsWith("'") && field.endsWith("'")) {
                        // Remove the single quotes
                        field = field.substring(1, field.length() - 1);
                    }
                    if(i==0){
                        listaIdInsertFileTabIssues.add(field.trim());
                    }else if(i==13){
                        listaDataCreazioneBugInsertFileTabIssues.add(field.trim());
                    }
                }
            }
        }
    }

    /**Confronta se i valori delle query insert ottenuti dal dump sono identici ai valori delle query
     * ottenute dalla divisione per massimo 30 righe di insert dalla insert query del dump
     *@params listaValuesDumpFile - lista valori insert dal dump
     *  listaValuesDumpSplitted - lista valori query insert splittati per massimo 30 values
     * @return true se le liste sono identiche
     *  */
    public boolean checkQueryValuesFromDumpEqualsQueryValuesSplittate(List<String> listaValuesRigheInsertDumpFile, List<String> listaValuesRigheInsertSplitted){
        boolean b = false;
        if(listaValuesRigheInsertDumpFile.size()==listaValuesRigheInsertSplitted.size()){
            for(int i = 0;i<listaValuesRigheInsertDumpFile.size();i++){
                String s1 = listaValuesRigheInsertDumpFile.get(i);
                String s2 = listaValuesRigheInsertSplitted.get(i);
                if(!s1.equals(s2)){
                    System.out.println(s1+"\n"+s2);
                    return false;
                }
            }
        }
        return true;
    }
    public List<String> getListaFinalQueries() {
        return listaFinalQueries;
    }

    public String getQueryAllInsertTabIssuesNonOrdinate() {
        return queryAllInsertNonOrdinate;
    }
    public LinkedHashSet<String> getListaIdInsertFileTabIssues() {
        return listaIdInsertFileTabIssues;
    }

    public LinkedHashSet<String> getListaDataCreazioneBugInsertFileTabIssues() {
        return listaDataCreazioneBugInsertFileTabIssues;
    }

    public LinkedHashSet<String> getListaIDInsertSplittedTabIssues() {
        return listaIDInsertSplittedTabIssues;
    }

    public LinkedHashSet<String> getListaDataCreazioneBugInsertSplittedTabIssues() {
        return listaDataCreazioneBugInsertSplittedTabIssues;
    }
}

