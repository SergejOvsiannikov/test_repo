# Пример сервиса

module Orders
  class RecreateWithFreeLimit
    BALANCE_FROM = 100_000
    START_DAYS = 45
    STEP = 45
    END_DAYS = 720

    def call
      return unless FeatureToggle.active?(:enable_send_companies_with_free_limit_to_amo)

      create_hidden_orders
    end

    private

    def create_hidden_orders
      company_scope.each do |company|
        next if company.orders.without_hidden_scoring.active.exists?
        next if company.loans.exists?(aasm_state: Loan::INVEST_STATES)

        # NOTE: в send_to_expire нужно прокидывать ActiveRecord::Relation коллекцию
        order_collection = company.orders.hidden_scoring.active.order(id: :desc).limit(1)

        send_to_expire(order_collection) if order_collection.present?

        additional_data = { free_available_limit: true }

        ::Flow::HiddenScorings::CreateJob.perform_later(
          order_id: company.orders.last.id,
          additional_data: additional_data
        )
      end
    end

    # Ищем компании с доступным лимитов выше BALANCE_FROM
    # Не в холде, у которых есть займы с активным гашением
    # и дата выдачи денег по последнему займу укладывается в days_range
    # если от текущей даты вычесть дату выдачи денег
    # без факторинга и поток холдингов
    def company_scope
      Company.joins(:company_limit)
             .joins(
               "INNER JOIN (
                SELECT DISTINCT ON (pvp_company_id) *
                FROM pvp_loans
                WHERE pvp_loans.aasm_state IN ('billing', 'repaid')
                ORDER BY pvp_company_id, money_sent_at DESC
              ) pvp_loans ON pvp_companies.id = pvp_loans.pvp_company_id ".squish
             )
             .left_joins(:orders)
             .where(hold_manual_scoring: false)
             .where(company_limits: { available: BALANCE_FROM.. })
             .where("('#{Time.zone.today}'::date - pvp_loans.money_sent_at::date) IN (#{days_range.join(',')})")
             .where.not(orders: { loan_type: [LoanType::FACTORING, LoanType::HIDDEN_FACTORING] })
             .where.not(inn: exclude_inns)
             # Обычный where.not(loan_type: [...]) исключает из выборки loan_type: nil, который нам нужен
             .where.not(
               "orders.flow_type IS NOT DISTINCT FROM '#{Orders::FlowType::POTOK_HOLDING}' OR
                 orders.flow_type IS NOT DISTINCT FROM '#{Orders::FlowType::POTOK_FINANCE}'"
             )
             .distinct
    end

    def days_range
      START_DAYS.step(by: STEP, to: END_DAYS).to_a
    end

    def exclude_inns
      Settings.potok_holding.borrowers.keys + Settings.potok_finance.borrowers.keys
    end

    def send_to_expire(order_collection)
      Orders::ExpireService.call(orders: order_collection, force: true)
    end
  end
end

# Пример active-job

module AmoCrm
  class SendEntityJob < ApplicationJob
    SENDER_CLASSES = {
      order: AmoHub::SendOrder,
      company: AmoHub::SendCompany,
      contact: AmoHub::SendContact,
      comment: AmoHub::SendComment
    }.freeze

    def perform(type:, order_id:)
      order = Order.find(order_id)

      SENDER_CLASSES[type].call(order)
    end
  end
end
