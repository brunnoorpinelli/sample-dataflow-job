package com.example.dataflow;

import com.google.cloud.spanner.Mutation;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline didático: lê um CSV de vendas no Cloud Storage, calcula o valor
 * total de cada venda (quantidade * preco_unitario) e grava o resultado na
 * tabela `vendas` do Cloud Spanner.
 *
 * Formato esperado do CSV (com cabeçalho):
 *   venda_id,produto,quantidade,preco_unitario
 *   V001,Teclado Mecanico,3,250.00
 */
public class CsvToSpannerPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(CsvToSpannerPipeline.class);

  /** Opções customizadas do pipeline, passadas via linha de comando. */
  public interface Options extends DataflowPipelineOptions {

    @Description("Caminho do CSV de entrada, ex: gs://meu-bucket/input/vendas.csv")
    @Validation.Required
    String getInputFile();

    void setInputFile(String value);

    @Description("ID da instância do Cloud Spanner")
    @Validation.Required
    String getInstanceId();

    void setInstanceId(String value);

    @Description("ID do banco de dados no Cloud Spanner")
    @Validation.Required
    String getDatabaseId();

    void setDatabaseId(String value);
  }

  /**
   * Transforma cada linha do CSV em uma Mutation do Spanner.
   * A "regra de negócio" didática: valor_total = quantidade * preco_unitario.
   */
  static class CsvParaMutationFn extends DoFn<String, Mutation> {

    @ProcessElement
    public void processElement(@Element String linha, OutputReceiver<Mutation> out) {
      // Ignora o cabeçalho e linhas vazias.
      if (linha.isBlank() || linha.startsWith("venda_id")) {
        return;
      }

      String[] campos = linha.split(",");
      if (campos.length != 4) {
        LOG.warn("Linha ignorada (esperados 4 campos): {}", linha);
        return;
      }

      String vendaId = campos[0].trim();
      String produto = campos[1].trim();
      long quantidade = Long.parseLong(campos[2].trim());
      double precoUnitario = Double.parseDouble(campos[3].trim());

      // O "cálculo" do job.
      double valorTotal = quantidade * precoUnitario;

      out.output(
          Mutation.newInsertOrUpdateBuilder("vendas")
              .set("venda_id").to(vendaId)
              .set("produto").to(produto)
              .set("quantidade").to(quantidade)
              .set("preco_unitario").to(precoUnitario)
              .set("valor_total").to(valorTotal)
              .build());
    }
  }

  public static void main(String[] args) {
    Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("LerCsvDoGcs", TextIO.read().from(options.getInputFile()))
        .apply("CalcularValorTotal", ParDo.of(new CsvParaMutationFn()))
        .apply(
            "GravarNoSpanner",
            SpannerIO.write()
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(options.getDatabaseId()));

    pipeline.run();
  }
}
