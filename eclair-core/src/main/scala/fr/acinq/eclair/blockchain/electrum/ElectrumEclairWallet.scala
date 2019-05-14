/*
 * Copyright 2018 ACINQ SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.acinq.eclair.blockchain.electrum

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import com.google.common.util.concurrent.{FutureCallback, Futures, MoreExecutors}
import fr.acinq.bitcoin.{ByteVector32, Satoshi, Script, Transaction, TxOut}
import fr.acinq.eclair.addressToPublicKeyScript
import fr.acinq.eclair.blockchain.electrum.ElectrumClient.BroadcastTransaction
import fr.acinq.eclair.blockchain.electrum.ElectrumWallet._
import fr.acinq.eclair.blockchain.{EclairWallet, MakeFundingTxResponse}
import grizzled.slf4j.Logging
import org.bitcoinj.core.PeerGroup
import org.bitcoinj.net.discovery.DnsDiscovery
import org.bitcoinj.params.TestNet3Params
import scodec.bits.ByteVector

import scala.concurrent.{ExecutionContext, Future, Promise}

class ElectrumEclairWallet(val wallet: ActorRef, chainHash: ByteVector32)(implicit system: ActorSystem, ec: ExecutionContext, timeout: akka.util.Timeout) extends EclairWallet with Logging {

  val params = TestNet3Params.get()
  val peerGroup = new PeerGroup(params, null)
  peerGroup.setMaxConnections(3)
  peerGroup.addPeerDiscovery(new DnsDiscovery(params))
  peerGroup.start()
  logger.info(s"bitcoinj peergroup started")

  override def getBalance = (wallet ? GetBalance).mapTo[GetBalanceResponse].map(balance => balance.confirmed + balance.unconfirmed)

  override def getFinalAddress = (wallet ? GetCurrentReceiveAddress).mapTo[GetCurrentReceiveAddressResponse].map(_.address)

  def getXpub: Future[GetXpubResponse] = (wallet ? GetXpub).mapTo[GetXpubResponse]

  override def makeFundingTx(pubkeyScript: ByteVector, amount: Satoshi, feeRatePerKw: Long): Future[MakeFundingTxResponse] = {
    val tx = Transaction(version = 2, txIn = Nil, txOut = TxOut(amount, pubkeyScript) :: Nil, lockTime = 0)
    (wallet ? CompleteTransaction(tx, feeRatePerKw)).mapTo[CompleteTransactionResponse].map(response => response match {
      case CompleteTransactionResponse(tx1, fee1, None) => MakeFundingTxResponse(tx1, 0, fee1)
      case CompleteTransactionResponse(_, _, Some(error)) => throw error
    })
  }

  override def commit(tx: Transaction): Future[Boolean] = {
    // this is to disambiguate bitcoin-lib's Transaction and bitcoinj's Transaction
    import org.bitcoinj.core.{Transaction => Transactionj}
    val broadcast = peerGroup.broadcastTransaction(new Transactionj(params, tx.bin.toArray))
    val res = Promise[Boolean]()
    Futures.addCallback(broadcast.future, new FutureCallback[Transactionj]() {
      override def onSuccess(transaction: Transactionj): Unit = {
        //tx broadcast successfully: commit tx
        // TODO: what if this ask fails?
        (wallet ? CommitTransaction(tx)) map {
          case CommitTransactionResponse(_) => res.success(true)
        }
      }
      def onFailure(throwable: Throwable): Unit = { // This can happen if we get a reject message from a peer.
        // TODO: make sure that the tx will never get published!
        //tx broadcast failed: cancel tx
        logger.error(s"cannot broadcast tx ${tx.txid}: ", throwable)
        // TODO: what if this ask fails?
        (wallet ? CancelTransaction(tx)) map {
          case CancelTransactionResponse(_) => res.success(false)
        }
      }
    }, MoreExecutors.directExecutor)
    res.future
  }

  def sendPayment(amount: Satoshi, address: String, feeRatePerKw: Long): Future[String] = {
    val publicKeyScript = Script.write(addressToPublicKeyScript(address, chainHash))
    val tx = Transaction(version = 2, txIn = Nil, txOut = TxOut(amount, publicKeyScript) :: Nil, lockTime = 0)

    (wallet ? CompleteTransaction(tx, feeRatePerKw))
      .mapTo[CompleteTransactionResponse]
      .flatMap {
        case CompleteTransactionResponse(tx, _, None) => commit(tx).map {
          case true => tx.txid.toString()
          case false => throw new RuntimeException(s"could not commit tx=$tx")
        }
        case CompleteTransactionResponse(_, _, Some(error)) => throw error
      }
  }

  def sendAll(address: String, feeRatePerKw: Long): Future[(Transaction, Satoshi)] = {
    val publicKeyScript = Script.write(addressToPublicKeyScript(address, chainHash))
    (wallet ? SendAll(publicKeyScript, feeRatePerKw))
      .mapTo[SendAllResponse]
      .map {
        case SendAllResponse(tx, fee) => (tx, fee)
      }
  }

  override def rollback(tx: Transaction): Future[Boolean] = (wallet ? CancelTransaction(tx)).map(_ => true)

  override def doubleSpent(tx: Transaction): Future[Boolean] = {
    (wallet ? IsDoubleSpent(tx)).mapTo[IsDoubleSpentResponse].map(_.isDoubleSpent)
  }
}
