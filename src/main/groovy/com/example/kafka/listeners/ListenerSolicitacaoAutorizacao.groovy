package com.example.kafka.listeners

import com.example.repository.ElasticSearchRepository
import groovy.transform.CompileStatic
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import jakarta.inject.Inject

import br.com.zgsolucoes.clients.autorizacoes.dominio.SolicitacaoAutorizacao

@KafkaListener(
		groupId = '${zgsolucoes.kafka.consumidores.sincroniaAutorizacoes.groupId:}',
		offsetStrategy = OffsetStrategy.ASYNC_PER_RECORD,
		offsetReset = OffsetReset.EARLIEST
)
@CompileStatic
class ListenerSolicitacaoAutorizacao {

	static final String INDEX_SOLICITACAO_AUTORIZACAO = 'solicitacao-autorizacao'

	@Inject
	ElasticSearchRepository<UUID, SolicitacaoAutorizacao> elasticSearchRepository

	@Topic('my-topic')
	def consomeAtualizacaoSolicitacaoAutorizacao(SolicitacaoAutorizacao solicitacao) {
		try {
			elasticSearchRepository.save(INDEX_SOLICITACAO_AUTORIZACAO, solicitacao.id, solicitacao)
//			elasticSearchRepository.get(INDEX_SOLICITACAO_AUTORIZACAO, SolicitacaoAutorizacao, UUID.fromString('3720bfa3-b3ad-4d48-888c-6d4fb7bcec14'))
//			elasticSearchRepository.delete(INDEX_SOLICITACAO_AUTORIZACAO, UUID.fromString('3720bfa3-b3ad-4d48-888c-6d4fb7bcec14'))
			elasticSearchRepository.search(INDEX_SOLICITACAO_AUTORIZACAO, SolicitacaoAutorizacao, 'id', UUID.fromString('3720bfa3-b3ad-4d48-888c-6d4fb7bcec14'))
		} catch (Exception ex) {
			throw new RuntimeException('Falhou ao inserir no elasticsearch', ex)
		}
	}

}
