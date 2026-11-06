// Apex Ledger: Atomic Transaction & Anti-Double Spending Service
import { Client } from 'pg';

export interface TransferRequest {
    idempotencyKey: string;
    sourceUserId: string;
    destinationUserId: string;
    amount: number;
}

export class WalletService {
    private db: Client;

    constructor(dbClient: Client) {
        this.db = dbClient;
    }

    async transferFunds(request: TransferRequest): Promise<boolean> {
        const { idempotencyKey, sourceUserId, destinationUserId, amount } = request;

        try {
            await this.db.query('BEGIN');

            // 1. Idempotency Check
            const checkDuplicate = await this.db.query(
                'SELECT id FROM transaction_ledger WHERE idempotency_key = $1',
                [idempotencyKey]
            );
            if (checkDuplicate.rows.length > 0) {
                throw new Error('Transaction already processed.');
            }

            // 2. High-Frequency Concurrent Balance Update with Pessimistic Locking
            const sourceWalletRes = await this.db.query(
                'SELECT id, balance FROM wallets WHERE user_id = $1 FOR UPDATE',
                [sourceUserId]
            );
            const destWalletRes = await this.db.query(
                'SELECT id, balance FROM wallets WHERE user_id = $1 FOR UPDATE',
                [destinationUserId]
            );

            if (sourceWalletRes.rows.length === 0 || destWalletRes.rows.length === 0) {
                throw new Error('Source or Destination wallet not found.');
            }

            const sourceWallet = sourceWalletRes.rows[0];
            const destWallet = destWalletRes.rows[0];

            if (Number(sourceWallet.balance) < amount) {
                throw new Error('Insufficient funds.');
            }

            // 3. Atomic Debit & Credit Operations
            await this.db.query(
                'UPDATE wallets SET balance = balance - $1 WHERE id = $2',
                [amount, sourceWallet.id]
            );
            await this.db.query(
                'UPDATE wallets SET balance = balance + $1 WHERE id = $2',
                [amount, destWallet.id]
            );

            // 4. Record to Audit Ledger
            await this.db.query(
                'INSERT INTO transaction_ledger (idempotency_key, source_wallet_id, destination_wallet_id, amount, status) VALUES ($1, $2, $3, $4, $5)',
                [idempotencyKey, sourceWallet.id, destWallet.id, amount, 'COMPLETED']
            );

            await this.db.query('COMMIT');
            return true;

        } catch (error) {
            await this.db.query('ROLLBACK');
            throw error;
        }
    }
}
